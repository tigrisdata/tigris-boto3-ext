"""Integration tests for bucket creation options."""

import pytest

from tigris_boto3_ext import (
    TigrisBucketOptions,
    TigrisSnapshotEnabled,
    create_bucket,
    get_bucket_metadata,
    has_snapshot_enabled,
    with_bucket_options,
)

from .conftest import anonymous_status, generate_bucket_name

ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"


def _public_grants(s3_client, bucket_name):
    grants = s3_client.get_bucket_acl(Bucket=bucket_name)["Grants"]
    return [g["Permission"] for g in grants if g["Grantee"].get("URI") == ALL_USERS]


class TestCreateBucketDefaults:
    def test_no_options_keeps_gateway_defaults(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Without options the bucket is private, STANDARD and global."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "create-default-")
        cleanup_buckets.append(bucket_name)

        create_bucket(s3_client, bucket_name)

        metadata = get_bucket_metadata(s3_client, bucket_name)
        assert metadata["storage_class"] == "STANDARD"
        assert metadata["regions"] == []
        assert _public_grants(s3_client, bucket_name) == []


class TestCreateBucketOptions:
    def test_default_storage_class(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """The bucket's default storage class is set from the header."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "create-tier-")
        cleanup_buckets.append(bucket_name)

        create_bucket(s3_client, bucket_name, default_storage_class="STANDARD_IA")

        assert (
            get_bucket_metadata(s3_client, bucket_name)["storage_class"]
            == "STANDARD_IA"
        )

    @pytest.mark.parametrize(
        ("regions", "expected"),
        [("iad", ["iad"]), (["iad", "sjc"], ["iad", "sjc"]), ("usa", ["usa"])],
        ids=["single", "dual", "multi"],
    )
    def test_regions(
        self, s3_client, test_bucket_prefix, cleanup_buckets, regions, expected
    ):
        """Single, dual and multi-region placement are echoed back by the gateway."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "create-region-")
        cleanup_buckets.append(bucket_name)

        create_bucket(s3_client, bucket_name, regions=regions)

        assert get_bucket_metadata(s3_client, bucket_name)["regions"] == expected

    def test_public_access_reads_but_does_not_list(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """access='public' grants anonymous reads; listing stays private by default."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "create-public-")
        cleanup_buckets.append(bucket_name)

        create_bucket(s3_client, bucket_name, access="public")
        s3_client.put_object(Bucket=bucket_name, Key="hello.txt", Body=b"hi")

        assert _public_grants(s3_client, bucket_name) == ["READ"]
        assert anonymous_status(bucket_name, "hello.txt") == 200
        assert anonymous_status(bucket_name) == 403

    def test_public_list_objects(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """public_list_objects=True lets anonymous requests list the bucket."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "create-listing-")
        cleanup_buckets.append(bucket_name)

        create_bucket(s3_client, bucket_name, access="public", public_list_objects=True)
        s3_client.put_object(Bucket=bucket_name, Key="hello.txt", Body=b"hi")

        assert anonymous_status(bucket_name) == 200

    def test_private_bucket_rejects_anonymous_reads(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """access='private' is the gateway default: no anonymous access at all."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "create-private-")
        cleanup_buckets.append(bucket_name)

        create_bucket(s3_client, bucket_name, access="private")
        s3_client.put_object(Bucket=bucket_name, Key="hello.txt", Body=b"hi")

        assert anonymous_status(bucket_name, "hello.txt") == 403


class TestBucketOptionsContextAndDecorator:
    def test_context_manager(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Plain create_bucket calls inside the block get the options."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "create-ctx-")
        cleanup_buckets.append(bucket_name)

        with TigrisBucketOptions(s3_client, regions="iad"):
            s3_client.create_bucket(Bucket=bucket_name)

        assert get_bucket_metadata(s3_client, bucket_name)["regions"] == ["iad"]

    def test_decorator(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """The decorated function's create_bucket gets the options."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "create-deco-")
        cleanup_buckets.append(bucket_name)

        @with_bucket_options(default_storage_class="STANDARD_IA")
        def make_bucket(client, name):
            return client.create_bucket(Bucket=name)

        make_bucket(s3_client, bucket_name)

        assert (
            get_bucket_metadata(s3_client, bucket_name)["storage_class"]
            == "STANDARD_IA"
        )

    def test_combines_with_snapshot_enabled(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Creation options nest with the other CreateBucket context managers."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "create-combo-")
        cleanup_buckets.append(bucket_name)

        with TigrisSnapshotEnabled(s3_client):
            create_bucket(s3_client, bucket_name, access="public", regions="iad")

        assert has_snapshot_enabled(s3_client, bucket_name) is True
        assert get_bucket_metadata(s3_client, bucket_name)["regions"] == ["iad"]
        assert _public_grants(s3_client, bucket_name) == ["READ"]
