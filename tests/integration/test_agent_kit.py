"""Integration tests for agent_kit helpers (run against real Tigris)."""

import time

from .conftest import bucket_exists, generate_bucket_name

from tigris_boto3_ext import (
    Checkpoint,
    Fork,
    ForkSet,
    Workspace,
    checkpoint,
    create_forks,
    create_workspace,
    list_checkpoints,
    restore,
    setup_coordination,
    teardown_coordination,
    teardown_forks,
    teardown_workspace,
)


class TestWorkspace:
    def test_create_basic_workspace(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        name = generate_bucket_name(test_bucket_prefix, "ws-basic-")
        cleanup_buckets.append(name)

        ws = create_workspace(s3_client, name)

        assert isinstance(ws, Workspace)
        assert ws.bucket == name
        assert bucket_exists(s3_client, name)

    def test_create_workspace_with_snapshots(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        name = generate_bucket_name(test_bucket_prefix, "ws-snap-")
        cleanup_buckets.append(name)

        ws = create_workspace(s3_client, name, enable_snapshots=True)

        assert ws.bucket == name
        # If snapshots are enabled the head_bucket exposes the flag.
        head = s3_client.head_bucket(Bucket=name)
        headers = head.get("ResponseMetadata", {}).get("HTTPHeaders", {})
        assert (
            str(headers.get("x-tigris-enable-snapshot", "")).lower() == "true"
        )

    def test_create_workspace_with_ttl(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        name = generate_bucket_name(test_bucket_prefix, "ws-ttl-")
        cleanup_buckets.append(name)

        # Just assert the call succeeds — Tigris stores the lifecycle rule
        # internally; we don't have a read endpoint here to verify it.
        ws = create_workspace(s3_client, name, ttl_days=1)
        assert ws.bucket == name
        assert bucket_exists(s3_client, name)

    def test_teardown_workspace_deletes_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        name = generate_bucket_name(test_bucket_prefix, "ws-down-")
        cleanup_buckets.append(name)

        ws = create_workspace(s3_client, name)
        s3_client.put_object(Bucket=name, Key="leftover.txt", Body=b"hi")

        teardown_workspace(s3_client, ws)

        assert not bucket_exists(s3_client, name)


class TestForks:
    def test_create_forks_creates_n_buckets(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        base = generate_bucket_name(test_bucket_prefix, "forks-base-")
        cleanup_buckets.append(base)

        # Base bucket must have snapshots enabled.
        create_workspace(s3_client, base, enable_snapshots=True)
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
            # The seed object copied over via the snapshot should be readable.
            obj = s3_client.get_object(Bucket=fork.bucket, Key="seed.txt")
            assert obj["Body"].read() == b"seed-data"

    def test_forks_are_isolated(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        base = generate_bucket_name(test_bucket_prefix, "forks-iso-")
        cleanup_buckets.append(base)
        create_workspace(s3_client, base, enable_snapshots=True)
        s3_client.put_object(Bucket=base, Key="shared.txt", Body=b"v1")

        prefix = generate_bucket_name(test_bucket_prefix, "fork-iso-")
        result = create_forks(s3_client, base, 2, prefix=prefix)
        for fork in result.forks:
            cleanup_buckets.append(fork.bucket)

        # Write divergent content into each fork.
        s3_client.put_object(
            Bucket=result.forks[0].bucket, Key="shared.txt", Body=b"fork-0"
        )
        s3_client.put_object(
            Bucket=result.forks[1].bucket, Key="shared.txt", Body=b"fork-1"
        )

        # Each fork sees its own write.
        f0 = s3_client.get_object(
            Bucket=result.forks[0].bucket, Key="shared.txt"
        )["Body"].read()
        f1 = s3_client.get_object(
            Bucket=result.forks[1].bucket, Key="shared.txt"
        )["Body"].read()
        assert f0 == b"fork-0"
        assert f1 == b"fork-1"

        # Base is untouched.
        base_obj = s3_client.get_object(Bucket=base, Key="shared.txt")[
            "Body"
        ].read()
        assert base_obj == b"v1"

    def test_teardown_forks_deletes_all(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        base = generate_bucket_name(test_bucket_prefix, "forks-down-")
        cleanup_buckets.append(base)
        create_workspace(s3_client, base, enable_snapshots=True)

        prefix = generate_bucket_name(test_bucket_prefix, "fork-down-")
        result = create_forks(s3_client, base, 2, prefix=prefix)

        # Put an object in each fork to exercise force-empty.
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
        create_workspace(s3_client, bucket, enable_snapshots=True)

        ck = checkpoint(s3_client, bucket, name=f"epoch-{int(time.time())}")
        assert isinstance(ck, Checkpoint)
        assert ck.snapshot_id
        assert ck.name and ck.name.startswith("epoch-")
        assert ck.created_at is not None

    def test_restore_creates_new_fork(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "ck-restore-")
        cleanup_buckets.append(bucket)
        create_workspace(s3_client, bucket, enable_snapshots=True)
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

        # The restored fork holds the original value.
        body = s3_client.get_object(Bucket=restored, Key="data.txt")[
            "Body"
        ].read()
        assert body == b"original"

    def test_list_checkpoints_returns_all(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "ck-list-")
        cleanup_buckets.append(bucket)
        create_workspace(s3_client, bucket, enable_snapshots=True)

        ck1 = checkpoint(s3_client, bucket, name="alpha")
        ck2 = checkpoint(s3_client, bucket, name="beta")

        listed = list_checkpoints(s3_client, bucket)

        assert len(listed) >= 2
        ids = {c.snapshot_id for c in listed}
        assert ck1.snapshot_id in ids
        assert ck2.snapshot_id in ids


class TestCoordination:
    def test_setup_and_teardown(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        bucket = generate_bucket_name(test_bucket_prefix, "coord-")
        cleanup_buckets.append(bucket)
        create_workspace(s3_client, bucket)

        # Configure a webhook — the URL is never reached in this test, we
        # only verify Tigris accepts the configuration.
        setup_coordination(
            s3_client,
            bucket,
            webhook_url="https://example.com/webhook",
            event_filter='WHERE `key` REGEXP "^results/"',
            auth_token="test-token",
        )

        teardown_coordination(s3_client, bucket)
