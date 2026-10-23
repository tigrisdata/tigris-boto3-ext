"""Integration tests for archive restore.

A restore takes about an hour to complete, so these tests cover the archived and in-progress
states and the gateway's answers, not a completed restore.
"""

import pytest
from botocore.exceptions import ClientError

from tigris_boto3_ext import create_bucket, get_restore_info, restore_object

from .conftest import generate_bucket_name


def _code(exc_info):
    return exc_info.value.response["Error"]["Code"]


class TestArchiveRestore:
    def test_archived_object_restore_lifecycle(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """An archived object reports archived, then in-progress once a restore is requested."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "restore-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)
        s3_client.put_object(
            Bucket=bucket_name, Key="cold.txt", Body=b"archived", StorageClass="GLACIER"
        )

        assert get_restore_info(s3_client, bucket_name, "cold.txt") == {
            "status": "archived",
            "expires_at": None,
        }

        response = restore_object(s3_client, bucket_name, "cold.txt", days=1)

        assert response["ResponseMetadata"]["HTTPStatusCode"] == 202
        assert get_restore_info(s3_client, bucket_name, "cold.txt") == {
            "status": "in-progress",
            "expires_at": None,
        }

        with pytest.raises(ClientError) as exc_info:
            restore_object(s3_client, bucket_name, "cold.txt", days=1)
        assert _code(exc_info) == "RestoreAlreadyInProgress"

    def test_objects_that_need_no_restore(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """STANDARD and GLACIER_IR objects have no restore state; GLACIER_IR refuses a restore."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "restore-none-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="std.txt", Body=b"standard")
        s3_client.put_object(
            Bucket=bucket_name, Key="ir.txt", Body=b"instant", StorageClass="GLACIER_IR"
        )

        assert get_restore_info(s3_client, bucket_name, "std.txt") is None
        assert get_restore_info(s3_client, bucket_name, "ir.txt") is None

        with pytest.raises(ClientError) as exc_info:
            restore_object(s3_client, bucket_name, "ir.txt")
        assert _code(exc_info) == "InvalidObjectState"

    def test_missing_key(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Both helpers surface the gateway's 404 for a missing key."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "restore-missing-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)

        with pytest.raises(ClientError) as exc_info:
            restore_object(s3_client, bucket_name, "nope.txt")
        assert _code(exc_info) == "NoSuchKey"

        with pytest.raises(ClientError) as exc_info:
            get_restore_info(s3_client, bucket_name, "nope.txt")
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
