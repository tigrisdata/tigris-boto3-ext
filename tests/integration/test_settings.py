"""Integration tests for updating bucket settings."""

import pytest

from tigris_boto3_ext import (
    TigrisAPIError,
    create_bucket,
    get_bucket_metadata,
    list_buckets,
    update_bucket,
)

from .conftest import generate_bucket_name

ALL_USERS = "http://acs.amazonaws.com/groups/global/AllUsers"


def _visibility(s3_client, bucket_name):
    # Names sort together, so a cursor just before the name reaches it in one page.
    page = list_buckets(
        s3_client,
        include_visibility=True,
        max_buckets=50,
        continuation_token=bucket_name[:-1],
    )
    return next(b["visibility"] for b in page["buckets"] if b["name"] == bucket_name)


def _public_grants(s3_client, bucket_name):
    grants = s3_client.get_bucket_acl(Bucket=bucket_name)["Grants"]
    return [g["Permission"] for g in grants if g["Grantee"].get("URI") == ALL_USERS]


class TestUpdateBucketSettings:
    def test_storage_class_regions_and_cache_control(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Body settings are applied together and read back from the metadata endpoint."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "settings-body-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)

        result = update_bucket(
            s3_client,
            bucket_name,
            default_storage_class="STANDARD_IA",
            regions=["iad", "sjc"],
            cache_control="max-age=60",
        )

        assert result["name"] == bucket_name
        assert result["raw"] == {"Bucket": bucket_name, "Update": "success"}
        metadata = get_bucket_metadata(s3_client, bucket_name)
        assert metadata["storage_class"] == "STANDARD_IA"
        assert metadata["regions"] == ["iad", "sjc"]
        assert metadata["cache_control"] == "max-age=60"

    def test_clearing_regions_and_cache_control(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Omitted settings are left alone; [] returns to global placement and '' clears Cache-Control."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "settings-clear-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name, regions="iad")

        update_bucket(s3_client, bucket_name, cache_control="max-age=60")

        metadata = get_bucket_metadata(s3_client, bucket_name)
        assert metadata["regions"] == ["iad"]
        assert metadata["cache_control"] == "max-age=60"

        update_bucket(s3_client, bucket_name, regions=[], cache_control="")

        metadata = get_bucket_metadata(s3_client, bucket_name)
        assert metadata["regions"] == []
        assert metadata["cache_control"] is None

    def test_object_acl_and_nosniff(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Object ACLs and the nosniff header toggle on and off."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "settings-headers-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)

        update_bucket(s3_client, bucket_name, allow_object_acl=True, nosniff=True)
        metadata = get_bucket_metadata(s3_client, bucket_name)
        assert metadata["allow_object_acl"] is True
        assert metadata["raw"]["additional_http_headers"] == {
            "X-Content-Type-Options": "nosniff"
        }

        update_bucket(s3_client, bucket_name, allow_object_acl=False, nosniff=False)
        metadata = get_bucket_metadata(s3_client, bucket_name)
        assert metadata["allow_object_acl"] is False
        assert not metadata["raw"].get("additional_http_headers")

    def test_access(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Access changes show in the bucket ACL and the listing at once, in both directions."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "settings-access-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)
        assert _public_grants(s3_client, bucket_name) == []
        assert _visibility(s3_client, bucket_name) == "private"

        update_bucket(s3_client, bucket_name, access="public", public_list_objects=True)

        assert _public_grants(s3_client, bucket_name) == ["READ"]
        assert _visibility(s3_client, bucket_name) == "public"

        update_bucket(
            s3_client, bucket_name, access="private", public_list_objects=False
        )

        assert _public_grants(s3_client, bucket_name) == []
        assert _visibility(s3_client, bucket_name) == "private"
        # The public domain follows these changes asynchronously and is not polled here.

    def test_unverified_custom_domain_is_rejected(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """A custom domain without a CNAME pointing at Tigris is refused by the gateway."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "settings-domain-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)

        with pytest.raises(TigrisAPIError) as exc_info:
            update_bucket(
                s3_client, bucket_name, custom_domain=f"{bucket_name}.example.com"
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.code == "InvalidArgument"
        assert get_bucket_metadata(s3_client, bucket_name)["custom_domain"] is None

    def test_missing_bucket(self, s3_client, test_bucket_prefix):
        """Updating a bucket that does not exist raises NoSuchBucket."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "settings-missing-")

        with pytest.raises(TigrisAPIError) as exc_info:
            update_bucket(s3_client, bucket_name, cache_control="max-age=60")

        assert exc_info.value.status_code == 404
        assert exc_info.value.code == "NoSuchBucket"
