"""Primitive-by-primitive walkthrough of the agent_kit surface.

Mirrors the shape of the other ``examples/*.py`` files: each top-level
function demonstrates one feature, the ``__main__`` runs them in order.

Run against real Tigris by exporting:

    export AWS_ENDPOINT_URL_S3=https://t3.storage.dev
    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...

then::

    uv run python examples/agent_kit_usage.py
"""

from __future__ import annotations

import os
import time
import uuid

import boto3

from tigris_boto3_ext import (
    Checkpoint,
    Fork,
    ForkSet,
    Workspace,
    checkpoint,
    clear_object_notifications,
    create_forks,
    create_workspace,
    restore,
    set_object_notifications,
    teardown_forks,
    teardown_workspace,
)


def make_client():
    """Build a boto3 S3 client from standard AWS env vars."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_ENDPOINT_URL")
        or "https://t3.storage.dev",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "your-access-key"),
        aws_secret_access_key=os.environ.get(
            "AWS_SECRET_ACCESS_KEY", "your-secret-key"
        ),
        region_name=os.environ.get("AWS_REGION", "auto"),
    )


s3 = make_client()


def _name(prefix: str) -> str:
    """Generate a short, unique bucket name for one example run."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def example_workspace_minimal() -> None:
    """Minimal workspace lifecycle: create, use, tear down."""
    print("\n=== Workspace: minimal ===")
    bucket = _name("ak-ws")

    ws: Workspace = create_workspace(s3, bucket)
    print(f"Created workspace at bucket={ws.bucket}")
    print(f"  snapshots enabled by default; credentials={ws.credentials}")

    s3.put_object(Bucket=ws.bucket, Key="hello.txt", Body=b"hi")
    print("  wrote hello.txt to the workspace")

    teardown_workspace(s3, ws)
    print("  torn down (force-delete via Tigris-Force-Delete header)")


def example_workspace_with_ttl_and_credentials() -> None:
    """Workspace with TTL and a bucket-scoped Editor access key."""
    print("\n=== Workspace: TTL + scoped credentials ===")
    bucket = _name("ak-ws")

    ws = create_workspace(
        s3,
        bucket,
        ttl_days=1,
        credentials_role="Editor",
    )
    print(f"Created workspace at bucket={ws.bucket}")
    if ws.credentials:
        print(f"  scoped access key id: {ws.credentials.access_key_id[:6]}...")
        print(f"  policy ARN: {ws.credentials.policy_arn}")
        print("  (these credentials only have access to this one bucket)")

    teardown_workspace(s3, ws)
    print("  torn down (revokes credentials, deletes bucket)")


def example_forks_parallel_runs() -> None:
    """Snapshot a base bucket then fork it N times for parallel agents."""
    print("\n=== Forks: parallel runs ===")
    base = _name("ak-base")
    create_workspace(s3, base)
    s3.put_object(Bucket=base, Key="seed.txt", Body=b"shared-seed")

    forks: ForkSet = create_forks(
        s3,
        base,
        count=3,
        prefix=_name("ak-fk"),
    )
    print(
        f"Snapshotted {forks.base_bucket} ({forks.snapshot_id}) → {len(forks.forks)} forks"
    )

    # Each fork is independent — divergent writes don't affect the base or peers.
    for i, fork in enumerate(forks.forks):
        s3.put_object(Bucket=fork.bucket, Key="result.txt", Body=f"agent-{i}".encode())
    for i, fork in enumerate(forks.forks):
        body = s3.get_object(Bucket=fork.bucket, Key="result.txt")["Body"].read()
        print(f"  fork {i}: bucket={fork.bucket}  result={body.decode()!r}")

    base_seed = s3.get_object(Bucket=base, Key="seed.txt")["Body"].read()
    print(f"  base bucket untouched: seed={base_seed.decode()!r}")

    teardown_forks(s3, forks)
    teardown_workspace(s3, Workspace(bucket=base))


def example_checkpoint_and_restore() -> None:
    """Capture a checkpoint, mutate the bucket, then restore into a fresh fork."""
    print("\n=== Checkpoints: capture + restore ===")
    bucket = _name("ak-ck")
    ws = create_workspace(s3, bucket)
    s3.put_object(Bucket=bucket, Key="state.json", Body=b'{"step": 1}')

    ck: Checkpoint = checkpoint(s3, bucket, name=f"epoch-{int(time.time())}")
    print(f"Checkpoint snapshot_id={ck.snapshot_id}  name={ck.name}")

    # Drift the live bucket forward.
    s3.put_object(Bucket=bucket, Key="state.json", Body=b'{"step": 2}')
    s3.put_object(Bucket=bucket, Key="state.json", Body=b'{"step": 3}')

    restored_bucket = restore(s3, bucket, ck.snapshot_id)
    print(f"Restored point-in-time fork: {restored_bucket}")

    body = s3.get_object(Bucket=restored_bucket, Key="state.json")["Body"].read()
    print(f"  restored state.json={body.decode()!r}  (expected step=1)")
    body_now = s3.get_object(Bucket=bucket, Key="state.json")["Body"].read()
    print(f"  live state.json={body_now.decode()!r}  (drifted to step=3)")

    # Tear down both: restored fork first, then the workspace.
    teardown_forks(
        s3,
        ForkSet(
            base_bucket=bucket,
            snapshot_id=ck.snapshot_id,
            forks=[Fork(bucket=restored_bucket)],
        ),
    )
    teardown_workspace(s3, ws)


def example_object_notifications() -> None:
    """Wire up + clear an object-event webhook on a bucket."""
    print("\n=== Object notifications ===")
    bucket = _name("ak-notif")
    ws = create_workspace(s3, bucket)

    set_object_notifications(
        s3,
        bucket,
        webhook_url="https://example.com/webhook",
        event_filter='WHERE `key` REGEXP "^results/"',
        auth_token="placeholder-token",
    )
    print(f"Notifications configured on {bucket}")

    clear_object_notifications(s3, bucket)
    print("Notifications cleared")

    teardown_workspace(s3, ws)


if __name__ == "__main__":
    print("tigris-boto3-ext — agent_kit usage examples")
    print("=" * 50)
    example_workspace_minimal()
    example_workspace_with_ttl_and_credentials()
    example_forks_parallel_runs()
    example_checkpoint_and_restore()
    example_object_notifications()
    print("\n" + "=" * 50)
    print("Done.")
