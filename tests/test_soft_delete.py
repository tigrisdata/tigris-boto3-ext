"""Unit tests for soft delete bucket creation."""

import pytest

from tigris_boto3_ext import (
    TigrisSoftDeleteEnabled,
    TigrisSoftDeleteView,
    create_soft_delete_bucket,
    purge_deleted_object,
    soft_delete_enabled,
    with_soft_delete_view,
)

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


SOFT_DELETE_EVENTS = [
    "before-sign.s3.DeleteObject",
    "before-sign.s3.ListObjectVersions",
    "before-sign.s3.ListObjectsV2",
]


class TestTigrisSoftDeleteView:
    def test_registers_delete_and_list_operations(
        self, mock_s3_client, mock_request_class
    ):
        with TigrisSoftDeleteView(mock_s3_client):
            calls = mock_s3_client.meta.events.register.call_args_list
            assert [call[0][0] for call in calls] == SOFT_DELETE_EVENTS
            for call in calls:
                request = mock_request_class()
                call[0][1](request)
                assert request.headers == {"X-Tigris-Soft-Delete": "true"}

        assert mock_s3_client.meta.events.unregister.call_count == 3


class TestWithSoftDeleteViewDecorator:
    def test_wraps_function_in_soft_delete_view(self, mock_s3_client):
        @with_soft_delete_view
        def registered_events(client, bucket):
            calls = client.meta.events.register.call_args_list
            return [call[0][0] for call in calls]

        events = registered_events(mock_s3_client, "my-bucket")

        assert events == SOFT_DELETE_EVENTS
        assert mock_s3_client.meta.events.unregister.call_count == 3


class TestPurgeDeletedObjectHelper:
    def test_deletes_the_version_inside_the_soft_delete_view(self, mock_s3_client):
        mock_s3_client.delete_object.return_value = {"ResponseMetadata": {}}

        result = purge_deleted_object(mock_s3_client, "my-bucket", "file.txt", "123")

        assert result == {"ResponseMetadata": {}}
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="my-bucket", Key="file.txt", VersionId="123"
        )
        calls = mock_s3_client.meta.events.register.call_args_list
        assert [call[0][0] for call in calls] == SOFT_DELETE_EVENTS
        assert mock_s3_client.meta.events.unregister.call_count == 3

    @pytest.mark.parametrize(
        ("bucket_name", "key", "version_id", "message"),
        [
            ("", "file.txt", "123", "bucket_name is required"),
            ("my-bucket", "", "123", "key is required"),
            ("my-bucket", "file.txt", "", "version_id is required"),
        ],
    )
    def test_rejects_empty_arguments(
        self, mock_s3_client, bucket_name, key, version_id, message
    ):
        with pytest.raises(ValueError, match=message):
            purge_deleted_object(mock_s3_client, bucket_name, key, version_id)

        mock_s3_client.delete_object.assert_not_called()
        mock_s3_client.meta.events.register.assert_not_called()
