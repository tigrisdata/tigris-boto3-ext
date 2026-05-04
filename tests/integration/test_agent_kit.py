"""Integration tests for agent_kit helpers (run against real Tigris)."""

import time

from .conftest import bucket_exists, generate_bucket_name

from tigris_boto3_ext import (
    Checkpoint,
    Credentials,
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


class TestWorkspace:
    def test_create_workspace_default_enables_snapshots(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "ws-default-")
        cleanup_buckets.append(bucket)

        ws = create_workspace(s3_client, bucket)

        assert isinstance(ws, Workspace)
        assert ws.bucket == bucket
        assert bucket_exists(s3_client, bucket)
        # Snapshots are on by default — the head_bucket header reflects it.
        head = s3_client.head_bucket(Bucket=bucket)
        headers = head.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        assert (
            str(headers.get("x-tigris-enable-snapshot", "")).lower() == "true"
        )

    def test_create_workspace_disable_snapshots(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "ws-no-snap-")
        cleanup_buckets.append(bucket)

        ws = create_workspace(s3_client, bucket, enable_snapshots=False)
        assert ws.bucket == bucket
        assert bucket_exists(s3_client, bucket)

    def test_create_workspace_with_ttl(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "ws-ttl-")
        cleanup_buckets.append(bucket)

        ws = create_workspace(s3_client, bucket, ttl_days=1)
        assert ws.bucket == bucket
        # Verify the lifecycle rule was actually persisted by Tigris.
        config = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = config.get("Rules", [])
        assert any(
            rule.get("Expiration", {}).get("Days") == 1 for rule in rules
        )

    def test_teardown_workspace_force_deletes_non_empty_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "ws-down-")
        cleanup_buckets.append(bucket)

        ws = create_workspace(s3_client, bucket)
        s3_client.put_object(Bucket=bucket, Key="leftover.txt", Body=b"hi")

        # force=True (the default) uses Tigris's force-delete extension.
        teardown_workspace(s3_client, ws)
        assert not bucket_exists(s3_client, bucket)

    def test_workspace_with_credentials_round_trip(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "ws-cred-")
        cleanup_buckets.append(bucket)

        ws = create_workspace(s3_client, bucket, credentials_role="Editor")
        assert isinstance(ws.credentials, Credentials)
        assert ws.credentials.access_key_id
        assert ws.credentials.secret_access_key

        teardown_workspace(s3_client, ws)
        assert not bucket_exists(s3_client, bucket)


class TestForks:
    def test_create_forks_creates_n_buckets(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        base = generate_bucket_name(test_bucket_prefix, "forks-base-")
        cleanup_buckets.append(base)

        create_workspace(s3_client, base)
        s3_client.put_object(Bucket=base, Key="seed.txt", Body=b"seed-data")

        prefix = generate_bucket_name(test_bucket_prefix, "fork-")
        result = create_forks(s3_client, base, 3, prefix=prefix)
        for fork in result.forks:
            cleanup_buckets.append(fork.bucket)

        assert isinstance(result, ForkSet)
        assert result.base_bucket == base
        assert result.snapshot_id
        assert len(result.forks) == 3
        for i, fork in enumerate(result.forks):
            assert isinstance(fork, Fork)
            assert fork.bucket == f"{prefix}-{i}"
            assert bucket_exists(s3_client, fork.bucket)
            obj = s3_client.get_object(Bucket=fork.bucket, Key="seed.txt")
            assert obj["Body"].read() == b"seed-data"

    def test_default_fork_prefix_uses_snapshot_id(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        # Tigris snapshot ids are ~19 chars; use a short suffix so the
        # default prefix + snapshot id + "-N" fits the 63-char bucket-name
        # limit (the standard test prefix + uuid is already ~34 chars).
        base = generate_bucket_name(test_bucket_prefix, "f-")
        cleanup_buckets.append(base)
        create_workspace(s3_client, base)

        result = create_forks(s3_client, base, 1)
        for fork in result.forks:
            cleanup_buckets.append(fork.bucket)

        assert result.forks[0].bucket == f"{base}-fork-{result.snapshot_id}-0"

    def test_forks_are_isolated(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        base = generate_bucket_name(test_bucket_prefix, "forks-iso-")
        cleanup_buckets.append(base)
        create_workspace(s3_client, base)
        s3_client.put_object(Bucket=base, Key="shared.txt", Body=b"v1")

        prefix = generate_bucket_name(test_bucket_prefix, "fork-iso-")
        result = create_forks(s3_client, base, 2, prefix=prefix)
        for fork in result.forks:
            cleanup_buckets.append(fork.bucket)

        s3_client.put_object(
            Bucket=result.forks[0].bucket, Key="shared.txt", Body=b"fork-0"
        )
        s3_client.put_object(
            Bucket=result.forks[1].bucket, Key="shared.txt", Body=b"fork-1"
        )

        assert (
            s3_client.get_object(
                Bucket=result.forks[0].bucket, Key="shared.txt"
            )["Body"].read()
            == b"fork-0"
        )
        assert (
            s3_client.get_object(
                Bucket=result.forks[1].bucket, Key="shared.txt"
            )["Body"].read()
            == b"fork-1"
        )
        # Base bucket is untouched.
        assert (
            s3_client.get_object(Bucket=base, Key="shared.txt")["Body"].read()
            == b"v1"
        )

    def test_create_forks_with_credentials(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        base = generate_bucket_name(test_bucket_prefix, "forks-cred-")
        cleanup_buckets.append(base)
        create_workspace(s3_client, base)

        prefix = generate_bucket_name(test_bucket_prefix, "fork-cred-")
        result = create_forks(
            s3_client, base, 2, prefix=prefix, credentials_role="ReadOnly"
        )
        for fork in result.forks:
            cleanup_buckets.append(fork.bucket)

        for fork in result.forks:
            assert isinstance(fork.credentials, Credentials)
            assert fork.credentials.access_key_id
            assert fork.credentials.secret_access_key
        ids = [f.credentials.access_key_id for f in result.forks]
        assert len(set(ids)) == len(ids)

        teardown_forks(s3_client, result)

    def test_teardown_forks_force_deletes_non_empty(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        base = generate_bucket_name(test_bucket_prefix, "forks-down-")
        cleanup_buckets.append(base)
        create_workspace(s3_client, base)

        prefix = generate_bucket_name(test_bucket_prefix, "fork-down-")
        result = create_forks(s3_client, base, 2, prefix=prefix)

        for fork in result.forks:
            s3_client.put_object(Bucket=fork.bucket, Key="x", Body=b"x")

        teardown_forks(s3_client, result)

        for fork in result.forks:
            assert not bucket_exists(s3_client, fork.bucket)


class TestCheckpoints:
    def test_checkpoint_returns_snapshot_id(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "ck-")
        cleanup_buckets.append(bucket)
        create_workspace(s3_client, bucket)

        ck = checkpoint(s3_client, bucket, name=f"epoch-{int(time.time())}")
        assert isinstance(ck, Checkpoint)
        assert ck.snapshot_id
        assert ck.name is not None
        assert ck.name.startswith("epoch-")
        assert ck.created_at is not None

    def test_restore_creates_new_fork(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "ck-restore-")
        cleanup_buckets.append(bucket)
        create_workspace(s3_client, bucket)
        s3_client.put_object(Bucket=bucket, Key="data.txt", Body=b"original")

        ck = checkpoint(s3_client, bucket)

        # Mutate the original after the checkpoint.
        s3_client.put_object(Bucket=bucket, Key="data.txt", Body=b"changed")

        restored = restore(
            s3_client,
            bucket,
            ck.snapshot_id,
            fork_name=generate_bucket_name(test_bucket_prefix, "ck-restored-"),
        )
        cleanup_buckets.append(restored)

        body = s3_client.get_object(Bucket=restored, Key="data.txt")[
            "Body"
        ].read()
        assert body == b"original"


class TestObjectNotifications:
    def test_set_and_clear(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "notif-")
        cleanup_buckets.append(bucket)
        create_workspace(s3_client, bucket)

        # The webhook URL is never invoked here — we only verify Tigris
        # accepts the configuration.
        set_object_notifications(
            s3_client,
            bucket,
            webhook_url="https://example.com/webhook",
            event_filter='WHERE `key` REGEXP "^results/"',
            auth_token="test-token",  # noqa: S106
        )
        clear_object_notifications(s3_client, bucket)
