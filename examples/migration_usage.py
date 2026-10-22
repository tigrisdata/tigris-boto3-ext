"""Examples for data migration with tigris-boto3-ext.

A Tigris bucket can take over a bucket at another S3-compatible provider
without an upfront copy: reads of objects Tigris does not have yet are served
from the source (the shadow bucket) and copied in the background. The source
settings and credentials come from your environment here; the gateway checks
them against the source before storing anything.
See https://www.tigrisdata.com/docs/migration/
"""

import os
from typing import Any

import boto3

from tigris_boto3_ext import (
    TigrisAPIError,
    TigrisListSource,
    create_bucket,
    force_delete_bucket,
    get_bucket_metadata,
    is_object_migrated,
    list_objects_from_source,
    migrate_object,
    set_bucket_migration,
    with_list_source,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)

BUCKET = "migration-demo"


def example_configure_source():
    """Point the bucket at the source bucket; stops here if the credentials are refused."""
    print("\n=== Configure the migration source ===")

    try:
        set_bucket_migration(
            s3,
            BUCKET,
            os.environ["SOURCE_BUCKET"],
            endpoint=os.environ["SOURCE_ENDPOINT"],
            region=os.environ.get("SOURCE_REGION", "auto"),
            access_key=os.environ["SOURCE_ACCESS_KEY"],
            secret_key=os.environ["SOURCE_SECRET_KEY"],
            write_through=True,
        )
    except TigrisAPIError as e:
        print(f"Source refused ({e.code}): {e}")
        return False
    shadow = get_bucket_metadata(s3, BUCKET)["raw"].get("shadow_bucket", {})
    print(f"Migrating from {shadow.get('name')} at {shadow.get('endpoint')}")
    return True


def example_drive_migration():
    """List what is still at the source and schedule its copy."""
    print("\n=== Drive the migration ===")

    remaining = list_objects_from_source(s3, BUCKET, "shadow", MaxKeys=10)
    for obj in remaining.get("Contents", []):
        key = obj["Key"]
        if is_object_migrated(s3, BUCKET, key):
            print(f"{key}: already in Tigris")
        else:
            migrate_object(s3, BUCKET, key)
            print(f"{key}: copy scheduled")

    with TigrisListSource(s3, "tigris"):
        migrated = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=10)
    print(f"{migrated.get('KeyCount', 0)} objects already in Tigris")

    @with_list_source("shadow")
    def source_keys(s3_client: Any, bucket: str) -> list[str]:
        page = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=10)
        return [o["Key"] for o in page.get("Contents", [])]

    print(f"At the source: {source_keys(s3, BUCKET)}")


def example_stop_migration():
    """Remove the source only once nothing is left that the source alone has."""
    print("\n=== Stop migrating ===")

    with TigrisListSource(s3, "shadow"):
        pages = s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET)
        pending = [
            obj["Key"]
            for page in pages
            for obj in page.get("Contents", [])
            if not is_object_migrated(s3, BUCKET, obj["Key"])
        ]
    if pending:
        print(
            f"Keeping the source: {len(pending)} objects are still only at the source"
        )
        return

    set_bucket_migration(s3, BUCKET, None)
    print(
        "shadow_bucket configured:",
        "shadow_bucket" in get_bucket_metadata(s3, BUCKET)["raw"],
    )


def example_validation():
    """Incomplete or malformed source settings are rejected before any request is sent."""
    print("\n=== Validation ===")

    for kwargs in (
        {"endpoint": "https://s3.amazonaws.com"},
        {
            "endpoint": "http://s3.amazonaws.com",
            "region": "us-east-1",
            "access_key": "k",
            "secret_key": "s",
        },
    ):
        try:
            set_bucket_migration(s3, BUCKET, "legacy", **kwargs)
        except ValueError as e:
            print(f"Rejected {sorted(kwargs)}: {e}")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Data Migration Usage Examples")
    print("=" * 50)

    create_bucket(s3, BUCKET)
    try:
        if example_configure_source():
            example_drive_migration()
            example_stop_migration()
        example_validation()
    finally:
        force_delete_bucket(s3, BUCKET)
