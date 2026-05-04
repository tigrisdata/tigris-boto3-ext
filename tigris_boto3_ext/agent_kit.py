"""Agent-storage workflow helpers: workspaces, parallel forks, and checkpoints.

A workspace is a Tigris bucket dedicated to a single agent — created with
snapshots enabled by default, optional TTL via the standard S3 lifecycle
API, and an optional bucket-scoped IAM access key. Forks are N independent
copy-on-write buckets created from one base-bucket snapshot. Checkpoints
are labeled snapshots that can be restored into a fresh fork.

For event-driven coordination, use :mod:`tigris_boto3_ext.object_notifications`.
"""

from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from typing import Literal

    from mypy_boto3_s3.client import S3Client

    Role = Literal["Editor", "ReadOnly"]
else:
    S3Client = object
    Role = str

from ._iam import create_scoped_access_key, delete_scoped_access_key
from .helpers import (
    create_fork,
    create_snapshot,
    create_snapshot_bucket,
    delete_bucket,
    get_snapshot_version,
)


@dataclass
class Credentials:
    """A scoped Tigris access key for a workspace or fork.

    ``user_name`` and ``policy_arn`` are bookkeeping for teardown — kept out
    of equality and repr so the user-facing key material is what shows up.
    """

    access_key_id: str
    secret_access_key: str
    user_name: Optional[str] = field(default=None, compare=False, repr=False)
    policy_arn: Optional[str] = field(default=None, compare=False, repr=False)


@dataclass
class Workspace:
    """A single-agent workspace."""

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
    """A labeled snapshot of a bucket.

    ``created_at`` is sourced client-side by :func:`checkpoint`
    (``datetime.now``), so it's excluded from equality so logically equal
    checkpoints from different sources still match.
    """

    snapshot_id: str
    name: Optional[str] = None
    created_at: Optional[datetime] = field(default=None, compare=False)


def create_workspace(
    s3_client: S3Client,
    bucket: str,
    *,
    ttl_days: Optional[int] = None,
    enable_snapshots: bool = True,
    credentials_role: Optional[Role] = None,
) -> Workspace:
    """Create a workspace for an agent.

    Args:
        s3_client: boto3 S3 client.
        bucket: Workspace bucket name.
        ttl_days: If set, configure a lifecycle rule that expires objects
            after this many days.
        enable_snapshots: If True (the default), enable snapshot support so
            checkpoints can be taken later.
        credentials_role: If set (``"Editor"`` or ``"ReadOnly"``), provision
            a bucket-scoped Tigris access key and attach it to
            ``Workspace.credentials``.

    Returns:
        The created Workspace.

    Usage:
        ws = create_workspace(
            s3, "agent-abc",
            ttl_days=1,
            credentials_role="Editor",
        )
        teardown_workspace(s3, ws)
    """
    if ttl_days is not None and ttl_days <= 0:
        msg = f"ttl_days must be positive, got {ttl_days}"
        raise ValueError(msg)
    _validate_role(credentials_role)

    if enable_snapshots:
        create_snapshot_bucket(s3_client, bucket)
    else:
        s3_client.create_bucket(Bucket=bucket)

    if ttl_days is not None:
        s3_client.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "workspace-ttl",
                        "Status": "Enabled",
                        "Filter": {"Prefix": ""},
                        "Expiration": {"Days": ttl_days},
                    },
                ],
            },
        )

    try:
        credentials = _provision_credentials(s3_client, bucket, credentials_role)
    except Exception:
        # Atomic semantics: if credentials were requested and provisioning
        # fails, roll back the bucket so the caller isn't left without a
        # handle to clean it up.
        with suppress(Exception):
            delete_bucket(s3_client, bucket, force=True)
        raise
    return Workspace(bucket=bucket, credentials=credentials)


def teardown_workspace(
    s3_client: S3Client,
    workspace: Workspace,
    *,
    force: bool = True,
) -> None:
    """Delete a workspace and revoke its scoped credentials, if any.

    With ``force=True`` (default) the bucket is force-deleted via the
    Tigris extension even if non-empty.
    """
    if workspace.credentials is not None:
        with suppress(Exception):
            delete_scoped_access_key(
                s3_client,
                access_key_id=workspace.credentials.access_key_id,
                user_name=workspace.credentials.user_name,
                policy_arn=workspace.credentials.policy_arn,
            )
    delete_bucket(s3_client, workspace.bucket, force=force)


def create_forks(
    s3_client: S3Client,
    base_bucket: str,
    count: int,
    *,
    prefix: Optional[str] = None,
    credentials_role: Optional[Role] = None,
) -> ForkSet:
    """Snapshot a bucket then create ``count`` independent copy-on-write forks.

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
            ``f"{base_bucket}-fork-{snapshot_id}"``.
        credentials_role: If set (``"Editor"`` or ``"ReadOnly"``), provision a
            bucket-scoped Tigris access key per fork.

    Returns:
        ForkSet with the base bucket, snapshot id, and a list of forks.

    Raises:
        ValueError: If ``count`` < 1 or ``credentials_role`` is unrecognized.
        RuntimeError: If the snapshot version could not be read or no forks
            could be created.
    """
    if count < 1:
        msg = f"count must be >= 1, got {count}"
        raise ValueError(msg)
    _validate_role(credentials_role)

    snapshot_response = create_snapshot(s3_client, base_bucket)
    snapshot_id = get_snapshot_version(snapshot_response)
    if snapshot_id is None:
        msg = f"Could not read snapshot version for base bucket {base_bucket!r}"
        raise RuntimeError(msg)

    fork_prefix = prefix or f"{base_bucket}-fork-{snapshot_id}"
    forks: list[Fork] = []

    for i in range(count):
        fork_name = f"{fork_prefix}-{i}"
        if not _try_create_fork(s3_client, fork_name, base_bucket, snapshot_id):
            # Best-effort: skip this fork and try the rest so a single
            # failure (e.g. a name collision) doesn't strand the others.
            continue
        # Provision credentials best-effort: the bucket already exists, so
        # we always append the Fork even if credential provisioning fails —
        # otherwise the caller has no handle to tear down the bucket.
        credentials = _try_provision_credentials(s3_client, fork_name, credentials_role)
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
    strand the rest. With ``force=True`` (default) each bucket is
    force-deleted via the Tigris extension even if non-empty.
    """
    for fork in fork_set.forks:
        if fork.credentials is not None:
            with suppress(Exception):
                delete_scoped_access_key(
                    s3_client,
                    access_key_id=fork.credentials.access_key_id,
                    user_name=fork.credentials.user_name,
                    policy_arn=fork.credentials.policy_arn,
                )
        with suppress(Exception):
            delete_bucket(s3_client, fork.bucket, force=force)


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
            ``f"{bucket}-restore-{snapshot_id}"`` so the resulting bucket
            name reflects which point-in-time it represents.

    Returns:
        Name of the new fork bucket.
    """
    new_name = fork_name or f"{bucket}-restore-{snapshot_id}"
    create_fork(s3_client, new_name, bucket, snapshot_version=snapshot_id)
    return new_name


def _validate_role(role: Optional[Role]) -> None:
    """Validate ``credentials_role`` upfront so a bad value can't strand a bucket.

    Called *before* any bucket-creating S3 call: otherwise the caller would
    get an exception on a bucket they were never handed back, leaking it.
    """
    if role is not None and role not in ("Editor", "ReadOnly"):
        msg = f"credentials_role must be 'Editor' or 'ReadOnly', got {role!r}"
        raise ValueError(msg)


def _provision_credentials(
    s3_client: S3Client, bucket: str, role: Optional[Role]
) -> Optional[Credentials]:
    """Create a Tigris scoped access key for ``bucket`` if a role is requested."""
    if role is None:
        return None
    key = create_scoped_access_key(s3_client, f"{bucket}-key", bucket, role)
    return Credentials(
        access_key_id=key["access_key_id"],
        secret_access_key=key["secret_access_key"],
        user_name=key.get("user_name"),
        policy_arn=key.get("policy_arn"),
    )


def _try_provision_credentials(
    s3_client: S3Client, bucket: str, role: Optional[Role]
) -> Optional[Credentials]:
    """Best-effort credential provisioning. Returns None on IAM failure.

    Used during fork creation: the bucket already exists, so we don't want
    a single IAM hiccup to strand the caller without a handle to the
    bucket. The fork is still tracked (without credentials) so teardown
    can clean it up. The role itself is validated upfront by the caller.
    """
    if role is None:
        return None
    try:
        return _provision_credentials(s3_client, bucket, role)
    except Exception:
        return None


def _try_create_fork(
    s3_client: S3Client,
    fork_name: str,
    base_bucket: str,
    snapshot_id: str,
) -> bool:
    """Best-effort fork creation. Returns True on success, False on failure."""
    try:
        create_fork(s3_client, fork_name, base_bucket, snapshot_version=snapshot_id)
    except Exception:
        return False
    return True
