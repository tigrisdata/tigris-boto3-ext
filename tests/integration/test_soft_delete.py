"""Integration tests for soft delete."""

from tigris_boto3_ext import (
    TigrisSoftDeleteView,
    create_soft_delete_bucket,
    purge_deleted_object,
    restore_deleted_object,
)

from .conftest import bucket_exists, generate_bucket_name


def _soft_deleted_versions(s3_client, bucket_name):
    """Versions currently in the bucket's soft-delete view."""
    with TigrisSoftDeleteView(s3_client):
        response = s3_client.list_object_versions(Bucket=bucket_name)
    return response.get("Versions", [])


class TestSoftDeleteBucketCreation:
    """Test creating buckets with soft delete enabled."""

    def test_deleted_object_is_recoverable(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """An object deleted from a soft-delete bucket lands in the soft-delete view."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "soft-delete-")
        cleanup_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name)
        assert bucket_exists(s3_client, bucket_name)

        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"data")
        s3_client.delete_object(Bucket=bucket_name, Key="file.txt")

        assert [v["Key"] for v in _soft_deleted_versions(s3_client, bucket_name)] == [
            "file.txt"
        ]

    def test_custom_retention_window(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """A custom retention window within 7 to 90 days is accepted."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "soft-delete-30-")
        cleanup_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name, retention_days=30)

        assert bucket_exists(s3_client, bucket_name)


class TestPurgeDeletedObject:
    """Test permanently deleting soft-deleted versions."""

    def test_purged_version_leaves_the_soft_delete_view(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Purging the only soft-deleted version empties the soft-delete view."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "purge-")
        cleanup_buckets.append(bucket_name)

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
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """A soft-deleted version restored by id is readable again."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "restore-")
        cleanup_buckets.append(bucket_name)

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
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Without a version id the most recent soft-deleted version comes back."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "restore-latest-")
        cleanup_buckets.append(bucket_name)

        create_soft_delete_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"data")
        s3_client.delete_object(Bucket=bucket_name, Key="file.txt")

        restore_deleted_object(s3_client, bucket_name, "file.txt")

        body = s3_client.get_object(Bucket=bucket_name, Key="file.txt")["Body"].read()
        assert body == b"data"
