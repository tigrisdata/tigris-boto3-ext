"""Integration tests for object notifications."""

import pytest

from tigris_boto3_ext import (
    TigrisAPIError,
    create_bucket,
    get_bucket_metadata,
    set_bucket_notifications,
)

from .conftest import generate_bucket_name

URL = "https://example.com/hooks/tigris"
FILTER = 'WHERE `Event-Type` = "OBJECT_DELETED"'
BEARER = "probe-token"
BASIC_AUTH = ("hook", "probe-pass")
CLEARED = {"enabled": False, "url": None, "filter": None, "auth": None}


def _notifications(s3_client, bucket_name):
    metadata = get_bucket_metadata(s3_client, bucket_name)
    return metadata["notifications"], metadata["raw"].get("object_notifications") or {}


class TestBucketNotifications:
    def test_configure_pause_and_remove(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Each call replaces the configuration; credentials are stored but read back masked."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "notif-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)
        assert _notifications(s3_client, bucket_name)[0] == CLEARED

        result = set_bucket_notifications(
            s3_client, bucket_name, URL, event_filter=FILTER, token=BEARER
        )

        assert result["name"] == bucket_name
        assert result["raw"] == {"Bucket": bucket_name, "Update": "success"}
        notifications, raw = _notifications(s3_client, bucket_name)
        assert notifications == {
            "enabled": True,
            "url": URL,
            "filter": FILTER,
            "auth": "token",
        }
        assert raw["auth"] == {"token": "****"}
        username, password = BASIC_AUTH

        set_bucket_notifications(
            s3_client,
            bucket_name,
            URL,
            enabled=False,
            username=username,
            password=password,
        )

        notifications, raw = _notifications(s3_client, bucket_name)
        assert notifications == {
            "enabled": False,
            "url": URL,
            "filter": None,
            "auth": "basic",
        }
        assert raw["auth"] == {"basic_user": username, "basic_pass": "****"}

        set_bucket_notifications(s3_client, bucket_name, None)

        assert _notifications(s3_client, bucket_name)[0] == CLEARED

    def test_gateway_rejects_a_malformed_filter(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Filter syntax is checked by the gateway; the configuration stays untouched."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "notif-filter-")
        cleanup_buckets.append(bucket_name)
        create_bucket(s3_client, bucket_name)

        with pytest.raises(TigrisAPIError) as exc_info:
            set_bucket_notifications(
                s3_client, bucket_name, URL, event_filter="this is not a filter ((("
            )

        assert exc_info.value.status_code == 400
        assert exc_info.value.code == "InvalidArgument"
        assert _notifications(s3_client, bucket_name)[0] == CLEARED

    def test_missing_bucket(self, s3_client, test_bucket_prefix):
        """Configuring a bucket that does not exist raises NoSuchBucket."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "notif-missing-")

        with pytest.raises(TigrisAPIError) as exc_info:
            set_bucket_notifications(s3_client, bucket_name, URL)

        assert exc_info.value.status_code == 404
        assert exc_info.value.code == "NoSuchBucket"
