"""Unit tests for bucket deletion: soft delete, force delete, deleted-bucket recovery."""

import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from tigris_boto3_ext import (
    TigrisAPIError,
    TigrisSoftDeleteEnabled,
    create_soft_delete_bucket,
    delete_snapshot,
    force_delete_bucket,
    list_deleted_buckets,
    restore_bucket,
    soft_delete_enabled,
)
from tigris_boto3_ext._internal import create_header_injector
from tigris_boto3_ext._transport import TigrisResponse

EVENT_NAME = "before-sign.s3.CreateBucket"


class TestTigrisSoftDeleteEnabled:
    @pytest.mark.parametrize(
        ("retention_days", "expected"),
        [(None, "true"), (7, "7"), (30, "30"), (90, "90")],
    )
    def test_injects_soft_delete_header(
        self, mock_s3_client, mock_request_class, retention_days, expected
    ):
        with TigrisSoftDeleteEnabled(mock_s3_client, retention_days):
            event_name, handler = mock_s3_client.meta.events.register.call_args[0]
            assert event_name == EVENT_NAME
            request = mock_request_class()
            handler(request)
            assert request.headers == {"X-Tigris-Soft-Delete": expected}

        mock_s3_client.meta.events.unregister.assert_called_once()

    @pytest.mark.parametrize("retention_days", [0, 6, 91, -1])
    def test_rejects_retention_outside_range(self, mock_s3_client, retention_days):
        with pytest.raises(ValueError, match="between 7 and 90"):
            TigrisSoftDeleteEnabled(mock_s3_client, retention_days)

        mock_s3_client.meta.events.register.assert_not_called()

    @pytest.mark.parametrize("retention_days", [7.5, "30", True])
    def test_rejects_non_integer_retention(self, mock_s3_client, retention_days):
        with pytest.raises(TypeError, match="whole number of days"):
            TigrisSoftDeleteEnabled(mock_s3_client, retention_days)

        mock_s3_client.meta.events.register.assert_not_called()

    def test_unregisters_when_create_bucket_raises(self, mock_s3_client):
        mock_s3_client.create_bucket.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            create_soft_delete_bucket(mock_s3_client, "my-bucket")

        mock_s3_client.meta.events.unregister.assert_called_once()


class TestSoftDeleteEnabledDecorator:
    def test_wraps_function_in_soft_delete_context(
        self, mock_s3_client, mock_request_class
    ):
        @soft_delete_enabled(retention_days=30)
        def make_bucket(client, name):
            event_name, handler = client.meta.events.register.call_args[0]
            request = mock_request_class()
            handler(request)
            client.create_bucket(Bucket=name)
            return event_name, request.headers

        event_name, headers = make_bucket(mock_s3_client, "my-bucket")

        assert event_name == EVENT_NAME
        assert headers == {"X-Tigris-Soft-Delete": "30"}
        mock_s3_client.create_bucket.assert_called_once_with(Bucket="my-bucket")
        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_default_window(self, mock_s3_client, mock_request_class):
        @soft_delete_enabled()
        def make_bucket(client, name):
            _, handler = client.meta.events.register.call_args[0]
            request = mock_request_class()
            handler(request)
            return request.headers

        headers = make_bucket(mock_s3_client, "my-bucket")

        assert headers == {"X-Tigris-Soft-Delete": "true"}


class TestCreateSoftDeleteBucketHelper:
    def test_creates_bucket_and_unregisters(self, mock_s3_client):
        mock_s3_client.create_bucket.return_value = {"Location": "/my-bucket"}

        result = create_soft_delete_bucket(mock_s3_client, "my-bucket", 30)

        assert result == {"Location": "/my-bucket"}
        mock_s3_client.create_bucket.assert_called_once_with(Bucket="my-bucket")
        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_rejects_invalid_retention_before_calling_s3(self, mock_s3_client):
        with pytest.raises(ValueError, match="between 7 and 90"):
            create_soft_delete_bucket(mock_s3_client, "my-bucket", 365)

        mock_s3_client.create_bucket.assert_not_called()


DELETE_BUCKET_EVENT = "before-sign.s3.DeleteBucket"


class TestForceDeleteBucketHelper:
    def test_injects_force_header_on_delete_bucket(
        self, mock_s3_client, mock_request_class
    ):
        mock_s3_client.delete_bucket.return_value = {"ResponseMetadata": {}}

        result = force_delete_bucket(mock_s3_client, "my-bucket")

        assert result == {"ResponseMetadata": {}}
        mock_s3_client.delete_bucket.assert_called_once_with(Bucket="my-bucket")
        event_name, handler = mock_s3_client.meta.events.register.call_args[0]
        assert event_name == DELETE_BUCKET_EVENT
        request = mock_request_class()
        handler(request)
        assert request.headers == {"X-Tigris-Force-Delete": "true"}
        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_unregisters_when_delete_bucket_raises(self, mock_s3_client):
        mock_s3_client.delete_bucket.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            force_delete_bucket(mock_s3_client, "my-bucket")

        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_refuses_to_overlap_with_another_bucket_deletion(self, mock_s3_client):
        other = create_header_injector(
            mock_s3_client, "DeleteBucket", {"X-Tigris-Snapshot-Version": "1"}
        )
        other.register()
        try:
            with pytest.raises(RuntimeError, match="already active"):
                force_delete_bucket(mock_s3_client, "my-bucket")
            mock_s3_client.delete_bucket.assert_not_called()
        finally:
            other.unregister()

    def test_rejects_empty_bucket_name(self, mock_s3_client):
        with pytest.raises(ValueError, match="bucket_name is required"):
            force_delete_bucket(mock_s3_client, "")

        mock_s3_client.delete_bucket.assert_not_called()

    def test_delete_snapshot_refuses_while_force_delete_is_active(self, mock_s3_client):
        other = create_header_injector(
            mock_s3_client, "DeleteBucket", {"X-Tigris-Force-Delete": "true"}
        )
        other.register()
        try:
            with pytest.raises(RuntimeError, match="in flight"):
                delete_snapshot(mock_s3_client, "my-bucket", "123")
            mock_s3_client.delete_bucket.assert_not_called()
        finally:
            other.unregister()


DELETED_PAGE = {
    "Owner": {"ID": "tid_x", "DisplayName": "tid_x"},
    "Buckets": {
        "Bucket": [
            {
                "Name": "cli-purge-1787763223",
                "CreationDate": "2026-08-26T16:54:03Z",
                "InitialCreatedDate": "2026-08-26T16:53:44Z",
                "SoftDeleteInfo": {"Enabled": True, "RetentionDays": 7},
            },
            {
                "Name": "cli-sd-1787756420",
                "CreationDate": "2026-08-26T15:11:14Z",
                "InitialCreatedDate": "2026-08-26T15:00:21Z",
                "SoftDeleteInfo": {"Enabled": True, "RetentionDays": 30},
            },
        ]
    },
    "ContinuationToken": "cli-sd-1787756420",
}
NEXT_PAGE = DELETED_PAGE["ContinuationToken"]  # an opaque cursor, not a secret


def _page(payload):
    return TigrisResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode(),
    )


class TestListDeletedBuckets:
    @patch("tigris_boto3_ext.buckets.delete.signed_request")
    def test_lists_a_page(self, mock_request, mock_s3_client):
        mock_request.return_value = _page(DELETED_PAGE)

        page = list_deleted_buckets(mock_s3_client, max_buckets=2)

        mock_request.assert_called_once_with(
            mock_s3_client,
            "GET",
            "",
            query={"OnlyDeleted": "true", "max-buckets": "2"},
            headers={"Accept": "application/json"},
        )
        assert page["buckets"] == [
            {
                "name": "cli-purge-1787763223",
                "created_at": datetime(2026, 8, 26, 16, 53, 44, tzinfo=timezone.utc),
                "modified_at": datetime(2026, 8, 26, 16, 54, 3, tzinfo=timezone.utc),
                "retention_days": 7,
            },
            {
                "name": "cli-sd-1787756420",
                "created_at": datetime(2026, 8, 26, 15, 0, 21, tzinfo=timezone.utc),
                "modified_at": datetime(2026, 8, 26, 15, 11, 14, tzinfo=timezone.utc),
                "retention_days": 30,
            },
        ]
        assert page["continuation_token"] == NEXT_PAGE
        assert page["raw"] == DELETED_PAGE

    @patch("tigris_boto3_ext.buckets.delete.signed_request")
    def test_passes_the_continuation_token(self, mock_request, mock_s3_client):
        mock_request.return_value = _page({"Buckets": {"Bucket": []}})

        page = list_deleted_buckets(mock_s3_client, continuation_token=NEXT_PAGE)

        assert mock_request.call_args[1]["query"] == {
            "OnlyDeleted": "true",
            "continuation-token": NEXT_PAGE,
        }
        assert page["buckets"] == []
        assert page["continuation_token"] is None

    @pytest.mark.parametrize(
        "payload",
        [{}, {"Buckets": {}}, {"Buckets": {"Bucket": None}}, {"Buckets": None}],
    )
    @patch("tigris_boto3_ext.buckets.delete.signed_request")
    def test_empty_listing_shapes(self, mock_request, mock_s3_client, payload):
        mock_request.return_value = _page(payload)

        page = list_deleted_buckets(mock_s3_client)

        assert page["buckets"] == []
        assert page["continuation_token"] is None

    @patch("tigris_boto3_ext.buckets.delete.signed_request")
    def test_single_entry_arrives_unwrapped(self, mock_request, mock_s3_client):
        mock_request.return_value = _page(
            {
                "Buckets": {
                    "Bucket": {
                        "Name": "only-one",
                        "SoftDeleteInfo": {"RetentionDays": 7},
                    }
                }
            }
        )

        page = list_deleted_buckets(mock_s3_client)

        assert [b["name"] for b in page["buckets"]] == ["only-one"]
        assert page["buckets"][0]["created_at"] is None

    @pytest.mark.parametrize("value", [0, -1, True, 2.5, "10"])
    def test_rejects_bad_page_sizes(self, mock_s3_client, value):
        with pytest.raises(ValueError, match="positive integer"):
            list_deleted_buckets(mock_s3_client, max_buckets=value)


class TestRestoreBucket:
    @patch("tigris_boto3_ext.buckets.delete.signed_request")
    def test_posts_restore(self, mock_request, mock_s3_client):
        mock_request.return_value = TigrisResponse(
            status_code=200, headers={"x-amz-request-id": "req-1"}
        )

        result = restore_bucket(mock_s3_client, "my-bucket")

        mock_request.assert_called_once_with(
            mock_s3_client, "POST", "my-bucket", query="restore"
        )
        assert result == {"name": "my-bucket", "request_id": "req-1"}

    @patch("tigris_boto3_ext.buckets.delete.signed_request")
    def test_gateway_errors_propagate(self, mock_request, mock_s3_client):
        mock_request.side_effect = TigrisAPIError(
            "Tigris request failed (HTTP 400): InvalidArgument: Invalid argument",
            status_code=400,
            body="{}",
            code="InvalidArgument",
        )

        with pytest.raises(TigrisAPIError) as exc_info:
            restore_bucket(mock_s3_client, "live-bucket")

        assert exc_info.value.code == "InvalidArgument"

    def test_rejects_empty_bucket_name(self, mock_s3_client):
        with pytest.raises(ValueError, match="bucket_name is required"):
            restore_bucket(mock_s3_client, "")
