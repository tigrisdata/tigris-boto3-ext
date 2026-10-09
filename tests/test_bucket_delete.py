"""Unit tests for bucket deletion: soft delete at creation and force delete."""

import pytest

from tigris_boto3_ext import (
    TigrisSoftDeleteEnabled,
    create_soft_delete_bucket,
    delete_snapshot,
    force_delete_bucket,
    soft_delete_enabled,
)
from tigris_boto3_ext._internal import create_header_injector

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
