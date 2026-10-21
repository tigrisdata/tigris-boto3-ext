"""Examples for object notifications with tigris-boto3-ext.

Tigris POSTs a JSON payload to a webhook when objects in a bucket are created,
deleted, renamed or transitioned. set_bucket_notifications configures that
webhook, with an optional event filter and bearer-token or basic authentication.
It is a Tigris endpoint rather than an S3 operation.
See https://www.tigrisdata.com/docs/buckets/object-notifications/
"""

import boto3

from tigris_boto3_ext import (
    TigrisAPIError,
    create_bucket,
    get_bucket_metadata,
    set_bucket_notifications,
)

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)

BUCKET = "notifications-demo"
WEBHOOK = "https://api.example.com/hooks/tigris"


def show():
    notifications = get_bucket_metadata(s3, BUCKET)["notifications"]
    print(
        f"enabled={notifications['enabled']} url={notifications['url']} "
        f"filter={notifications['filter']!r} auth={notifications['auth']}"
    )


def example_every_event():
    """Send every event to the webhook, authenticated with a bearer token."""
    print("\n=== Every event, token auth ===")

    set_bucket_notifications(s3, BUCKET, WEBHOOK, token="replace-with-your-token")
    show()


def example_filtered():
    """Only deletes under images/, authenticated with basic credentials."""
    print("\n=== Filtered, basic auth ===")

    set_bucket_notifications(
        s3,
        BUCKET,
        WEBHOOK,
        event_filter='WHERE `Event-Type` = "OBJECT_DELETED" AND `key` REGEXP "^images/"',
        username="tigris",
        password="replace-with-your-password",
    )
    show()


def example_pause_and_remove():
    """Pause delivery without losing the configuration, then remove it."""
    print("\n=== Pause, then remove ===")

    set_bucket_notifications(
        s3, BUCKET, WEBHOOK, enabled=False, token="replace-with-your-token"
    )
    show()
    set_bucket_notifications(s3, BUCKET, None)
    show()


def example_validation():
    """Bad arguments are rejected before any request; bad filters by the gateway."""
    print("\n=== Validation ===")

    for kwargs in (
        {"url": "http://api.example.com/hook"},
        {"url": WEBHOOK, "token": "t", "username": "u", "password": "p"},
        {"url": WEBHOOK, "username": "u"},
    ):
        try:
            set_bucket_notifications(s3, BUCKET, **kwargs)
        except ValueError as e:
            print(f"Rejected {kwargs}: {e}")

    try:
        set_bucket_notifications(s3, BUCKET, WEBHOOK, event_filter="not a filter (((")
    except TigrisAPIError as e:
        print(f"Gateway rejected the filter ({e.code}): {e}")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Object Notifications Usage Examples")
    print("=" * 50)

    create_bucket(s3, BUCKET)
    try:
        example_every_event()
        example_filtered()
        example_pause_and_remove()
        example_validation()
    finally:
        s3.delete_bucket(Bucket=BUCKET)
