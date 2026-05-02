"""Integration tests for object rename functionality."""

import pytest
from botocore.exceptions import ClientError

from .conftest import generate_bucket_name

from tigris_boto3_ext import TigrisRename, rename_object, with_rename


@pytest.fixture
def rename_bucket(s3_client, test_bucket_prefix, cleanup_buckets):
    """Create a fresh bucket for rename tests."""
    bucket_name = generate_bucket_name(test_bucket_prefix, "rename-")
    cleanup_buckets.append(bucket_name)
    s3_client.create_bucket(Bucket=bucket_name)
    return bucket_name


def _put(s3_client, bucket, key, body=b"hello"):
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)


def _object_exists(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


class TestRenameObjectHelper:
    def test_rename_moves_object_to_new_key(self, s3_client, rename_bucket):
        _put(s3_client, rename_bucket, "old-name.txt", b"contents")

        rename_object(s3_client, rename_bucket, "old-name.txt", "new-name.txt")

        assert not _object_exists(s3_client, rename_bucket, "old-name.txt")
        assert _object_exists(s3_client, rename_bucket, "new-name.txt")

        obj = s3_client.get_object(Bucket=rename_bucket, Key="new-name.txt")
        assert obj["Body"].read() == b"contents"

    def test_rename_works_with_nested_keys(self, s3_client, rename_bucket):
        _put(s3_client, rename_bucket, "dir/a.txt", b"data")

        rename_object(s3_client, rename_bucket, "dir/a.txt", "dir/b.txt")

        assert not _object_exists(s3_client, rename_bucket, "dir/a.txt")
        assert _object_exists(s3_client, rename_bucket, "dir/b.txt")

    def test_rename_works_with_special_character_keys(
        self, s3_client, rename_bucket
    ):
        """Keys containing characters that need URL-encoding must round-trip
        correctly — the helper passes CopySource as a dict so botocore
        handles the encoding."""
        src = "weird name + and #hash.txt"
        dst = "renamed weird name.txt"
        _put(s3_client, rename_bucket, src, b"payload")

        rename_object(s3_client, rename_bucket, src, dst)

        assert not _object_exists(s3_client, rename_bucket, src)
        assert _object_exists(s3_client, rename_bucket, dst)
        obj = s3_client.get_object(Bucket=rename_bucket, Key=dst)
        assert obj["Body"].read() == b"payload"


class TestRenameContextManager:
    def test_copy_object_inside_context_renames(self, s3_client, rename_bucket):
        _put(s3_client, rename_bucket, "src.txt", b"payload")

        with TigrisRename(s3_client):
            s3_client.copy_object(
                Bucket=rename_bucket,
                CopySource=f"{rename_bucket}/src.txt",
                Key="dst.txt",
            )

        assert not _object_exists(s3_client, rename_bucket, "src.txt")
        assert _object_exists(s3_client, rename_bucket, "dst.txt")

    def test_copy_object_outside_context_still_copies(
        self, s3_client, rename_bucket
    ):
        """The header must only be injected while the context is active."""
        _put(s3_client, rename_bucket, "src.txt", b"payload")

        s3_client.copy_object(
            Bucket=rename_bucket,
            CopySource=f"{rename_bucket}/src.txt",
            Key="copy.txt",
        )

        # A regular copy keeps the source.
        assert _object_exists(s3_client, rename_bucket, "src.txt")
        assert _object_exists(s3_client, rename_bucket, "copy.txt")


class TestWithRenameDecorator:
    def test_decorator_renames_inside_function(self, s3_client, rename_bucket):
        _put(s3_client, rename_bucket, "before.txt", b"x")

        @with_rename
        def do_rename(client, bucket, src, dst):
            return client.copy_object(
                Bucket=bucket, CopySource=f"{bucket}/{src}", Key=dst,
            )

        do_rename(s3_client, rename_bucket, "before.txt", "after.txt")

        assert not _object_exists(s3_client, rename_bucket, "before.txt")
        assert _object_exists(s3_client, rename_bucket, "after.txt")
