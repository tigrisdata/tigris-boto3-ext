"""Examples for updating bucket settings with tigris-boto3-ext.

A bucket's access, default storage class, placement, Cache-Control, custom
domain, response headers and snapshot type can be changed after creation. update_bucket
sends only the settings you pass. It is a Tigris endpoint rather than an S3
operation. See https://www.tigrisdata.com/docs/buckets/settings/
"""

import boto3

from tigris_boto3_ext import (
    TigrisAPIError,
    create_bucket,
    get_bucket_metadata,
    list_buckets,
    update_bucket,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)

BUCKET = "settings-demo"


def example_make_public():
    """Open a bucket to anonymous reads and listing, then check the listing."""
    print("\n=== Public access ===")

    update_bucket(s3, BUCKET, access="public", public_list_objects=True)
    page = list_buckets(s3, include_visibility=True)
    for bucket in page["buckets"]:
        if bucket["name"] == BUCKET:
            print(f"{BUCKET} is {bucket['visibility']}")
    print(f"Objects are served at https://{BUCKET}.t3.tigrisfiles.io/<key>")


def example_storage_and_placement():
    """Change where new objects go and which storage class they get."""
    print("\n=== Storage class and regions ===")

    update_bucket(
        s3,
        BUCKET,
        default_storage_class="STANDARD_IA",
        regions=["iad", "sjc"],
        cache_control="public, max-age=3600",
    )
    metadata = get_bucket_metadata(s3, BUCKET)
    print(
        f"{metadata['storage_class']} by default, data in {metadata['regions']}, "
        f"Cache-Control: {metadata['cache_control']}"
    )


def example_response_headers():
    """Let objects carry their own ACL and serve them with nosniff."""
    print("\n=== Object ACLs and headers ===")

    update_bucket(s3, BUCKET, allow_object_acl=True, nosniff=True)
    metadata = get_bucket_metadata(s3, BUCKET)
    print(
        f"allow_object_acl={metadata['allow_object_acl']}, "
        f"headers={metadata['raw'].get('additional_http_headers')}"
    )


def example_snapshot_type():
    """Turn an existing bucket into a snapshot bucket and back."""
    print("\n=== Snapshot type ===")

    update_bucket(s3, BUCKET, snapshot_enabled=True)
    print(f"snapshot_enabled={get_bucket_metadata(s3, BUCKET)['snapshot_enabled']}")
    update_bucket(s3, BUCKET, snapshot_enabled=False)
    print(f"snapshot_enabled={get_bucket_metadata(s3, BUCKET)['snapshot_enabled']}")


def example_custom_domain():
    """Serve the bucket from your own hostname; its CNAME must already point at the bucket."""
    print("\n=== Custom domain ===")

    try:
        update_bucket(s3, BUCKET, custom_domain="files.example.com")
        print(f"Serving from {get_bucket_metadata(s3, BUCKET)['custom_domain']}")
    except TigrisAPIError as e:
        print(
            f"Rejected ({e.code}): point the CNAME at {BUCKET}.t3.tigrisbucket.io first"
        )


def example_back_to_defaults():
    """Clear what the examples above set."""
    print("\n=== Back to defaults ===")

    update_bucket(
        s3,
        BUCKET,
        access="private",
        public_list_objects=False,
        allow_object_acl=False,
        default_storage_class="STANDARD",
        regions=[],
        cache_control="",
        nosniff=False,
    )
    metadata = get_bucket_metadata(s3, BUCKET)
    print(f"{metadata['storage_class']}, global, Cache-Control cleared")


def example_validation():
    """Bad settings are rejected before any request is sent."""
    print("\n=== Validation ===")

    for kwargs in (
        {},
        {"access": "everyone"},
        {"default_storage_class": "COLD"},
        {"nosniff": "yes"},
    ):
        try:
            update_bucket(s3, BUCKET, **kwargs)
        except (ValueError, TypeError) as e:
            print(f"Rejected {kwargs}: {e}")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Bucket Settings Usage Examples")
    print("=" * 50)

    create_bucket(s3, BUCKET)
    try:
        example_make_public()
        example_storage_and_placement()
        example_response_headers()
        example_snapshot_type()
        example_custom_domain()
        example_back_to_defaults()
        example_validation()
    finally:
        s3.delete_bucket(Bucket=BUCKET)
