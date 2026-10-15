"""Integration tests for soft delete."""

import contextlib
import time
from datetime import datetime

import pytest
from botocore.exceptions import ClientError

from tigris_boto3_ext import (
    TigrisAPIError,
    TigrisSnapshotEnabled,
    TigrisSoftDeleteEnabled,
    create_soft_delete_bucket,
    force_delete_bucket,
    list_deleted_buckets,
    list_deleted_object_versions,
    list_deleted_objects,
    purge_deleted_object,
    restore_bucket,
    restore_deleted_object,
)

from .conftest import bucket_exists, generate_bucket_name


def _soft_deleted_versions(s3_client, bucket_name):
    """Versions currently in the bucket's soft-delete view."""
    return list_deleted_object_versions(s3_client, bucket_name).get("Versions", [])


def _assert_bucket_gone(s3_client, bucket_name):
    """Fail unless head_bucket reports the bucket as missing, not some other error."""
    with pytest.raises(ClientError) as exc_info:
        s3_client.head_bucket(Bucket=bucket_name)
    assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


@pytest.fixture
def cleanup_soft_delete_buckets(s3_client, cleanup_buckets):
    """cleanup_buckets, plus a purge of soft-deleted versions before teardown."""
    yield cleanup_buckets

    for bucket_name in cleanup_buckets:
        with contextlib.suppress(ClientError):
            listing = s3_client.list_objects_v2(Bucket=bucket_name)
            for obj in listing.get("Contents", []):
                s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
            for version in _soft_deleted_versions(s3_client, bucket_name):
                purge_deleted_object(
                    s3_client, bucket_name, version["Key"], version["VersionId"]
                )


class TestSoftDeleteBucketCreation:
    """Test creating buckets with soft delete enabled."""

    def test_deleted_object_is_recoverable(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """An object deleted from a soft-delete bucket lands in the soft-delete view."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "soft-delete-")
        cleanup_soft_delete_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name)
        assert bucket_exists(s3_client, bucket_name)

        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"data")
        s3_client.delete_object(Bucket=bucket_name, Key="file.txt")

        assert [v["Key"] for v in _soft_deleted_versions(s3_client, bucket_name)] == [
            "file.txt"
        ]

    def test_custom_retention_window(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """A custom retention window within 7 to 90 days is accepted."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "soft-delete-8-")
        cleanup_soft_delete_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name, retention_days=8)

        assert bucket_exists(s3_client, bucket_name)


class TestPurgeDeletedObject:
    """Test permanently deleting soft-deleted versions."""

    def test_purged_version_leaves_the_soft_delete_view(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """Purging the only soft-deleted version empties the soft-delete view."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "purge-")
        cleanup_soft_delete_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"data")
        s3_client.delete_object(Bucket=bucket_name, Key="file.txt")

        deleted = _soft_deleted_versions(s3_client, bucket_name)
        assert [v["Key"] for v in deleted] == ["file.txt"]

        purge_deleted_object(
            s3_client, bucket_name, "file.txt", deleted[0]["VersionId"]
        )

        assert _soft_deleted_versions(s3_client, bucket_name) == []


class TestRestoreDeletedObject:
    """Test restoring soft-deleted objects."""

    def test_restores_a_specific_version(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """A soft-deleted version restored by id is readable again."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "restore-")
        cleanup_soft_delete_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"data")
        s3_client.delete_object(Bucket=bucket_name, Key="file.txt")

        deleted = _soft_deleted_versions(s3_client, bucket_name)
        restore_deleted_object(
            s3_client, bucket_name, "file.txt", deleted[0]["VersionId"]
        )

        body = s3_client.get_object(Bucket=bucket_name, Key="file.txt")["Body"].read()
        assert body == b"data"

    def test_restores_most_recent_version_without_an_id(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """Without a version id the most recent soft-deleted version comes back."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "restore-latest-")
        cleanup_soft_delete_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"data")
        s3_client.delete_object(Bucket=bucket_name, Key="file.txt")

        restore_deleted_object(s3_client, bucket_name, "file.txt")

        body = s3_client.get_object(Bucket=bucket_name, Key="file.txt")["Body"].read()
        assert body == b"data"


class TestListDeletedObjects:
    """Test listing soft-deleted objects and their versions."""

    def test_lists_only_deleted_keys_and_versions(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """Live objects stay out of the soft-delete view; deleted ones are in it."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "list-deleted-")
        cleanup_soft_delete_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="gone.txt", Body=b"1")
        s3_client.put_object(Bucket=bucket_name, Key="kept.txt", Body=b"2")
        s3_client.delete_object(Bucket=bucket_name, Key="gone.txt")

        objects = list_deleted_objects(s3_client, bucket_name)
        assert [o["Key"] for o in objects.get("Contents", [])] == ["gone.txt"]

        versions = list_deleted_object_versions(s3_client, bucket_name)["Versions"]
        assert [v["Key"] for v in versions] == ["gone.txt"]
        assert versions[0]["VersionId"]

    def test_snapshot_bucket_view_excludes_live_versions(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """On a snapshot bucket only the deleted version is listed, never the live one."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "list-deleted-snap-")
        cleanup_soft_delete_buckets.append(bucket_name)

        with TigrisSnapshotEnabled(s3_client), TigrisSoftDeleteEnabled(s3_client):
            s3_client.create_bucket(Bucket=bucket_name)
        old = s3_client.put_object(Bucket=bucket_name, Key="b.txt", Body=b"v1")[
            "VersionId"
        ]
        s3_client.put_object(Bucket=bucket_name, Key="b.txt", Body=b"v2")
        s3_client.delete_object(Bucket=bucket_name, Key="b.txt", VersionId=old)

        versions = list_deleted_object_versions(s3_client, bucket_name)["Versions"]
        assert [(v["Key"], v["VersionId"]) for v in versions] == [("b.txt", old)]


class TestForceDeleteBucket:
    """Test deleting non-empty buckets."""

    def test_removes_a_non_empty_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """A plain bucket with objects in it is deleted in one call."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "force-delete-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"data")

        force_delete_bucket(s3_client, bucket_name)

        assert not _assert_bucket_gone(s3_client, bucket_name)
        cleanup_buckets.remove(bucket_name)

    def test_removes_a_non_empty_soft_delete_bucket(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """A soft-delete bucket with objects leaves the live namespace in one call."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "force-delete-sd-")
        cleanup_soft_delete_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"data")

        force_delete_bucket(s3_client, bucket_name)

        assert not _assert_bucket_gone(s3_client, bucket_name)
        cleanup_soft_delete_buckets.remove(bucket_name)


def _all_deleted_buckets(s3_client):
    """Every entry of the deleted list, following the continuation tokens."""
    token = None
    while True:
        page = list_deleted_buckets(s3_client, max_buckets=200, continuation_token=token)
        yield from page["buckets"]
        token = page["continuation_token"]
        if not token:
            return


def _find_deleted_bucket(s3_client, bucket_name, timeout=15):
    """The deleted-list entry for the bucket, polling briefly in case the listing lags."""
    deadline = time.time() + timeout
    while True:
        for entry in _all_deleted_buckets(s3_client):
            if entry["name"] == bucket_name:
                return entry
        if time.time() > deadline:
            return None
        time.sleep(1)


class TestDeletedBuckets:
    """Test listing and restoring soft-deleted buckets."""

    def test_deleted_bucket_is_listed_and_restorable(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """A force-deleted soft-delete bucket shows up in the deleted list and comes back whole."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "softdel-restore-")
        cleanup_soft_delete_buckets.append(bucket_name)
        create_soft_delete_bucket(s3_client, bucket_name, retention_days=7)
        s3_client.put_object(Bucket=bucket_name, Key="keep.txt", Body=b"survives")

        force_delete_bucket(s3_client, bucket_name)
        _assert_bucket_gone(s3_client, bucket_name)

        entry = _find_deleted_bucket(s3_client, bucket_name)
        assert entry is not None, "deleted bucket not listed"
        assert entry["retention_days"] == 7
        assert isinstance(entry["created_at"], datetime)
        assert isinstance(entry["modified_at"], datetime)

        result = restore_bucket(s3_client, bucket_name)

        assert result["name"] == bucket_name
        assert bucket_exists(s3_client, bucket_name)
        body = s3_client.get_object(Bucket=bucket_name, Key="keep.txt")["Body"].read()
        assert body == b"survives"
        assert _find_deleted_bucket(s3_client, bucket_name, timeout=0) is None

    def test_listing_pages(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """max_buckets bounds the page and the continuation token moves it forward."""
        names = [
            generate_bucket_name(test_bucket_prefix, f"softdel-page{i}-") for i in (1, 2)
        ]
        cleanup_soft_delete_buckets.extend(names)
        for name in names:
            create_soft_delete_bucket(s3_client, name)
            force_delete_bucket(s3_client, name)
            # Soft-deleted on purpose; nothing left for the fixture to clean up.
            cleanup_soft_delete_buckets.remove(name)
        # Both must be visible before the paging assertions below can mean anything.
        for name in names:
            assert _find_deleted_bucket(s3_client, name) is not None, name

        # At least two deleted buckets exist now, so a page of one must be truncated.
        first = list_deleted_buckets(s3_client, max_buckets=1)
        assert len(first["buckets"]) == 1
        assert first["continuation_token"]

        second = list_deleted_buckets(
            s3_client, max_buckets=1, continuation_token=first["continuation_token"]
        )
        assert len(second["buckets"]) == 1
        assert second["buckets"][0]["name"] != first["buckets"][0]["name"]

    def test_restoring_a_live_bucket_is_rejected(
        self, s3_client, test_bucket_prefix, cleanup_soft_delete_buckets
    ):
        """Only soft-deleted buckets can be restored."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "softdel-live-")
        cleanup_soft_delete_buckets.append(bucket_name)
        create_soft_delete_bucket(s3_client, bucket_name)

        with pytest.raises(TigrisAPIError) as exc_info:
            restore_bucket(s3_client, bucket_name)

        assert exc_info.value.status_code == 400
        assert exc_info.value.code == "InvalidArgument"

    def test_restoring_an_unknown_bucket_raises(self, s3_client, test_bucket_prefix):
        """A bucket that never existed, or whose window expired, is NoSuchBucket."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "softdel-missing-")

        with pytest.raises(TigrisAPIError) as exc_info:
            restore_bucket(s3_client, bucket_name)

        assert exc_info.value.status_code == 404
        assert exc_info.value.code == "NoSuchBucket"
