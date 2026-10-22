"""Integration tests for data migration: the shadow bucket setting and the object helpers.

No external source bucket is involved: the gateway validates a source at configuration
time, so these tests cover its refusals, removal, and the object helpers on a bucket
without a source, where they are plain HeadObject and ListObjectsV2 calls.
"""

import pytest
from botocore.exceptions import ClientError

from tigris_boto3_ext import (
    TigrisAPIError,
    TigrisListSource,
    create_bucket,
    get_bucket_metadata,
    is_object_migrated,
    list_objects_from_source,
    migrate_object,
    set_bucket_migration,
    with_list_source,
)

from .conftest import generate_bucket_name

PLACEHOLDER = "unused"
EXTERNAL = {
    "endpoint": "https://s3.amazonaws.com",
    "region": "us-east-1",
    "access_key": "AKIAPROBEPROBEPROBE1",
    "secret_key": "not-a-real-secret-key-0000000000000000000",
}


class TestSetBucketMigration:
    def test_tigris_source_is_refused(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """A Tigris bucket cannot be a migration source."""
        prefix = generate_bucket_name(test_bucket_prefix, "migration-")
        bucket_name, source = f"{prefix}-target", f"{prefix}-source"
        cleanup_buckets.extend([bucket_name, source])
        create_bucket(s3_client, bucket_name)
        create_bucket(s3_client, source)

        with pytest.raises(TigrisAPIError) as exc_info:
            set_bucket_migration(
                s3_client,
                bucket_name,
                source,
                endpoint="https://t3.storage.dev",
                region="auto",
                access_key=PLACEHOLDER,
                secret_key=PLACEHOLDER,
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.code == "InvalidArgument"
        assert "Tigris endpoint" in str(exc_info.value)
        assert "shadow_bucket" not in get_bucket_metadata(s3_client, bucket_name)["raw"]

    def test_source_credentials_are_checked_before_storing(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """The gateway tries the source with the given credentials and relays its answer."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "migration-creds-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)

        with pytest.raises(TigrisAPIError) as exc_info:
            set_bucket_migration(s3_client, bucket_name, "no-such-source", **EXTERNAL)

        assert exc_info.value.status_code == 403
        assert exc_info.value.code == "InvalidAccessKeyId"
        assert "shadow_bucket" not in get_bucket_metadata(s3_client, bucket_name)["raw"]

    def test_remove_without_a_source(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Removing on a bucket that has no source is accepted."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "migration-none-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)

        result = set_bucket_migration(s3_client, bucket_name, None)

        assert result["raw"] == {"Bucket": bucket_name, "Update": "success"}
        assert "shadow_bucket" not in get_bucket_metadata(s3_client, bucket_name)["raw"]


class TestObjectMigrationHelpers:
    def test_without_a_source(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Objects of a bucket without a source count as migrated; the headers are accepted."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "migration-objects-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="a.txt", Body=b"a")

        assert is_object_migrated(s3_client, bucket_name, "a.txt") is True
        assert migrate_object(s3_client, bucket_name, "a.txt")["ContentLength"] == 1
        assert is_object_migrated(s3_client, bucket_name, "a.txt") is True

        with pytest.raises(ClientError) as exc_info:
            migrate_object(s3_client, bucket_name, "missing.txt")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404

        for source in ("tigris", "shadow"):
            listed = list_objects_from_source(s3_client, bucket_name, source)
            assert [o["Key"] for o in listed["Contents"]] == ["a.txt"]

        with TigrisListSource(s3_client, "shadow"):
            listed = s3_client.list_objects_v2(Bucket=bucket_name)
        assert [o["Key"] for o in listed["Contents"]] == ["a.txt"]

        @with_list_source("tigris")
        def keys(client, bucket):
            return [o["Key"] for o in client.list_objects_v2(Bucket=bucket)["Contents"]]

        assert keys(s3_client, bucket_name) == ["a.txt"]
