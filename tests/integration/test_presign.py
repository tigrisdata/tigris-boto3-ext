"""Integration tests for presigned URLs from the presign endpoint."""

import pytest
import urllib3
from botocore.exceptions import ClientError

from tigris_boto3_ext import (
    create_bucket,
    create_snapshot,
    create_snapshot_bucket,
    get_snapshot_version,
    presign,
)

from .conftest import generate_bucket_name

http = urllib3.PoolManager()


class TestPresign:
    def test_get_and_put_urls_work_on_a_private_bucket(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """The signed URLs are served from the public domain and need no further auth."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "presign-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="report.txt", Body=b"report")

        download = presign(s3_client, bucket_name, "report.txt", expires_in=300)

        assert download["operation"] == "get"
        assert download["expires_in"] == 300
        assert download["version_id"] is None
        assert download["custom_domain_url"] is None
        assert download["url"].startswith(f"https://{bucket_name}.")
        assert "X-Amz-Signature=" in download["url"]
        response = http.request("GET", download["url"])
        assert (response.status, response.data) == (200, b"report")

        upload = presign(s3_client, bucket_name, "incoming.txt", "put")

        assert upload["operation"] == "put"
        assert http.request("PUT", upload["url"], body=b"uploaded").status == 200
        assert (
            s3_client.get_object(Bucket=bucket_name, Key="incoming.txt")["Body"].read()
            == b"uploaded"
        )

    def test_snapshot_version_pins_the_read(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """A URL for a snapshot serves the object as it was then, not the live one."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "presign-snap-")
        cleanup_buckets.append(bucket_name)
        create_snapshot_bucket(s3_client, bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="a.txt", Body=b"v1")
        version = get_snapshot_version(
            create_snapshot(s3_client, bucket_name, snapshot_name="v1")
        )
        s3_client.put_object(Bucket=bucket_name, Key="a.txt", Body=b"v2")
        s3_client.put_object(Bucket=bucket_name, Key="later.txt", Body=b"new")

        pinned = presign(s3_client, bucket_name, "a.txt", snapshot_version=version)

        assert pinned["version_id"]
        assert f"versionId={pinned['version_id']}" in pinned["url"]
        assert http.request("GET", pinned["url"]).data == b"v1"
        assert (
            http.request("GET", presign(s3_client, bucket_name, "a.txt")["url"]).data
            == b"v2"
        )

        with pytest.raises(ClientError) as exc_info:
            presign(s3_client, bucket_name, "later.txt", snapshot_version=version)
        assert exc_info.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404
