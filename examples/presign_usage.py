"""Examples for presigned URLs with tigris-boto3-ext.

Tigris's presign endpoint signs a URL with your own access key and serves it
from the public domain, private buckets included; a download can be pinned to
the object as it was at a snapshot. boto3's generate_presigned_url also works
against Tigris; this endpoint adds the custom-domain URL and snapshot reads.
"""

import boto3
import urllib3

from tigris_boto3_ext import (
    create_snapshot,
    create_snapshot_bucket,
    force_delete_bucket,
    get_snapshot_version,
    presign,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)
http = urllib3.PoolManager()

BUCKET = "presign-demo"


def example_download_and_upload():
    """Sign a download and an upload; anyone with the URL can use it until it expires."""
    print("\n=== Download and upload URLs ===")

    s3.put_object(Bucket=BUCKET, Key="report.txt", Body=b"quarterly numbers")
    download = presign(s3, BUCKET, "report.txt", expires_in=600)
    print("download:", download["url"][:80], "...")
    print("fetched:", http.request("GET", download["url"]).data)

    upload = presign(s3, BUCKET, "incoming.txt", "put")
    http.request("PUT", upload["url"], body=b"sent by a browser")
    print("uploaded:", s3.get_object(Bucket=BUCKET, Key="incoming.txt")["Body"].read())


def example_snapshot_pinned_download():
    """A URL for the object as it was at a snapshot, even after it changed."""
    print("\n=== Snapshot-pinned download ===")

    s3.put_object(Bucket=BUCKET, Key="config.json", Body=b'{"version": 1}')
    snapshot = get_snapshot_version(create_snapshot(s3, BUCKET, snapshot_name="v1"))
    s3.put_object(Bucket=BUCKET, Key="config.json", Body=b'{"version": 2}')

    pinned = presign(s3, BUCKET, "config.json", snapshot_version=snapshot)
    print("object version at the snapshot:", pinned["version_id"])
    print("pinned:", http.request("GET", pinned["url"]).data)
    print(
        "live:  ", http.request("GET", presign(s3, BUCKET, "config.json")["url"]).data
    )


def example_validation():
    """Bad arguments are rejected before any request is sent."""
    print("\n=== Validation ===")

    for args, kwargs in (
        (("report.txt", "delete"), {}),
        (("report.txt",), {"expires_in": 0}),
        (("report.txt", "put"), {"snapshot_version": "1792757954482613190"}),
    ):
        try:
            presign(s3, BUCKET, *args, **kwargs)
        except ValueError as e:
            print(f"Rejected {args} {kwargs}: {e}")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Presigned URL Usage Examples")
    print("=" * 50)

    create_snapshot_bucket(s3, BUCKET)
    try:
        example_download_and_upload()
        example_snapshot_pinned_download()
        example_validation()
    finally:
        force_delete_bucket(s3, BUCKET)
