"""High-level helpers for AI-agent storage workflows on Tigris.

Mirrors the public surface of `@tigrisdata/agent-kit` (TypeScript) — workspaces,
forks, checkpoints, and coordination — composed from this library's lower-level
header-injection helpers and direct-HTTP REST helpers.
"""

import time
import uuid
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from typing import Literal

    from mypy_boto3_s3.client import S3Client

    Role = Literal["Editor", "ReadOnly"]
else:
    S3Client = object
    Role = str

from ._iam import create_access_key_with_buckets_role, delete_access_key
from ._rest import patch_bucket_settings
from .helpers import (
    create_fork,
    create_snapshot,
    create_snapshot_bucket,
    get_snapshot_version,
    list_snapshots,
)


@dataclass
class Credentials:
    """A scoped Tigris access key for a workspace or fork."""

    access_key_id: str
    secret_access_key: str


@dataclass
class Workspace:
    """A single-agent workspace bucket."""

    bucket: str
    credentials: Optional[Credentials] = None


@dataclass
class Fork:
    """A copy-on-write fork bucket."""

    bucket: str
    credentials: Optional[Credentials] = None


@dataclass
class ForkSet:
    """A set of forks created from a single base-bucket snapshot."""

    base_bucket: str
    snapshot_id: str
    forks: list[Fork] = field(default_factory=list)


@dataclass
class Checkpoint:
    """A labeled snapshot of a bucket."""

    snapshot_id: str
    name: Optional[str] = None
    created_at: Optional[datetime] = None


def create_workspace(
    s3_client: S3Client,
    name: str,
    *,
    ttl_days: Optional[int] = None,
    enable_snapshots: bool = False,
    credentials_role: Optional[Role] = None,
) -> Workspace:
    """Create a workspace bucket for an agent.

    Args:
        s3_client: boto3 S3 client.
        name: Bucket name.
        ttl_days: If set, configure a lifecycle rule that expires objects
            after this many days.
        enable_snapshots: If True, enable snapshot support on the bucket so
            checkpoints can be taken later.
        credentials_role: If set (``"Editor"`` or ``"ReadOnly"``), provision a
            scoped Tigris access key for the workspace bucket and return it
            on the workspace's ``credentials`` field.

    Returns:
        The created Workspace.

    Usage:
        ws = create_workspace(
            s3, "agent-abc",
            ttl_days=1,
            enable_snapshots=True,
            credentials_role="Editor",
        )
        # ws.credentials.access_key_id / .secret_access_key
        teardown_workspace(s3, ws)
    """
    if ttl_days is not None and ttl_days <= 0:
        msg = f"ttl_days must be positive, got {ttl_days}"
        raise ValueError(msg)

    if enable_snapshots:
        create_snapshot_bucket(s3_client, name)
    else:
        s3_client.create_bucket(Bucket=name)

    if ttl_days is not None:
        rule_id = f"workspace-ttl-{uuid.uuid4().hex[:12]}"
        patch_bucket_settings(
            s3_client,
            name,
            {
                "lifecycle_rules": [
                    {
                        "id": rule_id,
                        "expiration": {"days": ttl_days, "enabled": True},
                        "status": 1,
                    },
                ],
            },
        )

    credentials = _provision_credentials(s3_client, name, credentials_role)
    return Workspace(bucket=name, credentials=credentials)


def teardown_workspace(
    s3_client: S3Client,
    workspace: Workspace,
    *,
    force: bool = True,
) -> None:
    """Delete a workspace bucket and revoke its scoped credentials, if any.

    Args:
        s3_client: boto3 S3 client.
        workspace: Workspace returned by create_workspace.
        force: If True (default), empty the bucket before deletion.
    """
    if workspace.credentials is not None:
        with suppress(Exception):
            delete_access_key(s3_client, workspace.credentials.access_key_id)
    if force:
        _empty_bucket(s3_client, workspace.bucket)
    s3_client.delete_bucket(Bucket=workspace.bucket)


def create_forks(
    s3_client: S3Client,
    base_bucket: str,
    count: int,
    *,
    prefix: Optional[str] = None,
    credentials_role: Optional[Role] = None,
) -> ForkSet:
    """Snapshot a bucket then create `count` independent copy-on-write forks.

    Each fork is its own bucket; agents can read and write without affecting
    the base bucket or each other. Fork creation is instant regardless of
    base-bucket size.

    The base bucket must have snapshots enabled (see
    :func:`tigris_boto3_ext.create_snapshot_bucket`).

    Args:
        s3_client: boto3 S3 client.
        base_bucket: Bucket to fork from.
        count: Number of forks to create. Must be >= 1.
        prefix: Optional prefix for fork bucket names. Defaults to
            ``f"{base_bucket}-fork-{timestamp}"``.
        credentials_role: If set (``"Editor"`` or ``"ReadOnly"``), provision a
            scoped Tigris access key per fork and attach it to the
            corresponding ``Fork.credentials``.

    Returns:
        ForkSet with the base bucket, snapshot id, and a list of forks.

    Raises:
        ValueError: If ``count`` < 1.
        RuntimeError: If the snapshot version could not be read from the
            CreateSnapshot response, or if no forks could be created.
    """
    if count < 1:
        msg = f"count must be >= 1, got {count}"
        raise ValueError(msg)

    snapshot_response = create_snapshot(s3_client, base_bucket)
    snapshot_id = get_snapshot_version(snapshot_response)
    if snapshot_id is None:
        msg = f"Could not read snapshot version for base bucket {base_bucket!r}"
        raise RuntimeError(msg)

    fork_prefix = prefix or f"{base_bucket}-fork-{int(time.time())}"
    forks: list[Fork] = []

    for i in range(count):
        fork_name = f"{fork_prefix}-{i}"
        try:
            create_fork(
                s3_client,
                fork_name,
                base_bucket,
                snapshot_version=snapshot_id,
            )
        except Exception:
            # Stop creating more forks; return what we have.
            break
        credentials = _provision_credentials(s3_client, fork_name, credentials_role)
        forks.append(Fork(bucket=fork_name, credentials=credentials))

    if not forks:
        msg = f"Failed to create any forks of {base_bucket!r}"
        raise RuntimeError(msg)

    return ForkSet(base_bucket=base_bucket, snapshot_id=snapshot_id, forks=forks)


def teardown_forks(
    s3_client: S3Client,
    fork_set: ForkSet,
    *,
    force: bool = True,
) -> None:
    """Delete every fork in a ForkSet, revoking each fork's credentials.

    Best-effort: per-fork errors are swallowed so a single failure doesn't
    strand the rest. Use ``force=False`` to skip emptying buckets before
    deletion.
    """
    for fork in fork_set.forks:
        if fork.credentials is not None:
            with suppress(Exception):
                delete_access_key(s3_client, fork.credentials.access_key_id)
        if force:
            _empty_bucket(s3_client, fork.bucket)
        with suppress(Exception):
            s3_client.delete_bucket(Bucket=fork.bucket)


def checkpoint(
    s3_client: S3Client,
    bucket: str,
    *,
    name: Optional[str] = None,
) -> Checkpoint:
    """Capture a checkpoint (labeled snapshot) of a bucket.

    The bucket must have snapshots enabled.

    Args:
        s3_client: boto3 S3 client.
        bucket: Bucket to snapshot.
        name: Optional human-readable label (e.g. ``"epoch-50"``).

    Returns:
        Checkpoint with the snapshot id usable by :func:`restore`.
    """
    response = create_snapshot(s3_client, bucket, snapshot_name=name)
    snapshot_id = get_snapshot_version(response)
    if snapshot_id is None:
        msg = f"Could not read snapshot version for bucket {bucket!r}"
        raise RuntimeError(msg)
    return Checkpoint(
        snapshot_id=snapshot_id,
        name=name,
        created_at=datetime.now(timezone.utc),
    )


def restore(
    s3_client: S3Client,
    bucket: str,
    snapshot_id: str,
    *,
    fork_name: Optional[str] = None,
) -> str:
    """Restore from a checkpoint into a new fork.

    The original bucket is left untouched; this creates an independent
    copy-on-write fork at the snapshot point.

    Args:
        s3_client: boto3 S3 client.
        bucket: Bucket the checkpoint was taken on.
        snapshot_id: Snapshot id from :func:`checkpoint`.
        fork_name: Optional name for the new fork bucket. Defaults to
            ``f"{bucket}-restore-{timestamp}"``.

    Returns:
        Name of the new fork bucket.
    """
    new_name = fork_name or f"{bucket}-restore-{int(time.time())}"
    create_fork(s3_client, new_name, bucket, snapshot_version=snapshot_id)
    return new_name


def list_checkpoints(s3_client: S3Client, bucket: str) -> list[Checkpoint]:
    """List all checkpoints (snapshots) for a bucket.

    Each entry's snapshot id is parsed from the Tigris snapshot listing,
    where ``Name`` has the form ``"<version>"`` or ``"<version>; name=<label>"``.
    """
    response = list_snapshots(s3_client, bucket)
    checkpoints: list[Checkpoint] = []
    for entry in response.get("Buckets", []) or []:
        raw_name = entry.get("Name")
        if not raw_name:
            continue
        version, _, label = raw_name.partition("; name=")
        checkpoints.append(
            Checkpoint(
                snapshot_id=version,
                name=label or None,
                created_at=entry.get("CreationDate"),
            )
        )
    return checkpoints


def setup_coordination(
    s3_client: S3Client,
    bucket: str,
    *,
    webhook_url: str,
    event_filter: Optional[str] = None,
    auth_token: Optional[str] = None,
    auth_username: Optional[str] = None,
    auth_password: Optional[str] = None,
) -> None:
    """Configure a webhook that fires on object events in a bucket.

    Tigris will POST to ``webhook_url`` when objects are created, deleted, or
    modified.

    Args:
        s3_client: boto3 S3 client.
        bucket: Bucket to configure notifications on.
        webhook_url: HTTP/HTTPS endpoint that will receive notifications.
        event_filter: Optional Tigris filter expression
            (e.g. ``'WHERE `key` REGEXP "^results/"'``).
        auth_token: Bearer token sent with each webhook request. Mutually
            exclusive with basic auth.
        auth_username: HTTP basic auth username; must be paired with
            ``auth_password``.
        auth_password: HTTP basic auth password; must be paired with
            ``auth_username``.

    Raises:
        ValueError: For invalid arguments (empty webhook url, mixed auth modes,
            partial basic-auth credentials).
    """
    if not webhook_url:
        msg = "webhook_url is required"
        raise ValueError(msg)
    if auth_token is not None and (
        auth_username is not None or auth_password is not None
    ):
        msg = "auth_token cannot be combined with auth_username/auth_password"
        raise ValueError(msg)
    if (auth_username is None) != (auth_password is None):
        msg = "auth_username and auth_password must be provided together"
        raise ValueError(msg)

    notification: dict[str, Any] = {
        "enabled": True,
        "web_hook": webhook_url,
        "filter": event_filter or "",
    }
    if auth_token is not None:
        notification["auth"] = {"token": auth_token}
    elif auth_username is not None and auth_password is not None:
        notification["auth"] = {
            "basic_user": auth_username,
            "basic_pass": auth_password,
        }

    patch_bucket_settings(s3_client, bucket, {"object_notifications": notification})


def teardown_coordination(s3_client: S3Client, bucket: str) -> None:
    """Remove webhook notifications from a bucket."""
    patch_bucket_settings(s3_client, bucket, {"object_notifications": {}})


def _provision_credentials(
    s3_client: S3Client, bucket: str, role: Optional[Role]
) -> Optional[Credentials]:
    """Create a Tigris scoped access key for ``bucket`` if a role is requested."""
    if role is None:
        return None
    if role not in ("Editor", "ReadOnly"):
        msg = f"credentials_role must be 'Editor' or 'ReadOnly', got {role!r}"
        raise ValueError(msg)
    access_key = create_access_key_with_buckets_role(
        s3_client,
        f"{bucket}-key",
        [{"bucket": bucket, "role": role}],
    )
    return Credentials(
        access_key_id=access_key["access_key_id"],
        secret_access_key=access_key["secret_access_key"],
    )


def _empty_bucket(s3_client: S3Client, bucket: str) -> None:
    """Delete every object (including versions and delete markers) in a bucket."""
    if _empty_versioned(s3_client, bucket):
        return
    _empty_unversioned(s3_client, bucket)


def _empty_versioned(s3_client: S3Client, bucket: str) -> bool:
    """Empty a versioned bucket. Returns True on success, False on failure."""
    try:
        paginator = s3_client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket):
            objects: list[Any] = []
            for v in page.get("Versions", []) or []:
                objects.append({"Key": v["Key"], "VersionId": v["VersionId"]})
            for m in page.get("DeleteMarkers", []) or []:
                objects.append({"Key": m["Key"], "VersionId": m["VersionId"]})
            if objects:
                s3_client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
    except Exception:
        return False
    return True


def _empty_unversioned(s3_client: S3Client, bucket: str) -> None:
    """Empty a non-versioned bucket via list_objects_v2 — best-effort."""
    with suppress(Exception):
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []) or []:
                s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
