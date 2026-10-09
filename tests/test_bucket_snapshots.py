"""Unit tests for bucket snapshots."""

import pytest

from tigris_boto3_ext import delete_snapshot
from tigris_boto3_ext._internal import _handler_registry, create_header_injector

EVENT_NAME = "before-sign.s3.DeleteBucket"


class TestDeleteSnapshotHelper:
    def test_calls_delete_bucket_with_bucket_name(self, mock_s3_client):
        mock_s3_client.delete_bucket.return_value = {"ResponseMetadata": {}}

        result = delete_snapshot(mock_s3_client, "my-bucket", "1234567890")

        assert result == {"ResponseMetadata": {}}
        mock_s3_client.delete_bucket.assert_called_once_with(Bucket="my-bucket")

    def test_injects_snapshot_version_header_on_delete_bucket(
        self, mock_s3_client, mock_request_class
    ):
        delete_snapshot(mock_s3_client, "my-bucket", "1234567890")

        event_name, handler = mock_s3_client.meta.events.register.call_args[0]
        assert event_name == EVENT_NAME

        request = mock_request_class()
        handler(request)
        assert request.headers == {"X-Tigris-Snapshot-Version": "1234567890"}

    def test_unregisters_handler_after_call(self, mock_s3_client):
        delete_snapshot(mock_s3_client, "my-bucket", "1234567890")

        mock_s3_client.meta.events.unregister.assert_called_once()
        assert (id(mock_s3_client), EVENT_NAME) not in _handler_registry

    def test_unregisters_handler_when_delete_bucket_raises(self, mock_s3_client):
        mock_s3_client.delete_bucket.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            delete_snapshot(mock_s3_client, "my-bucket", "1234567890")

        mock_s3_client.meta.events.unregister.assert_called_once()
        assert (id(mock_s3_client), EVENT_NAME) not in _handler_registry

    @pytest.mark.parametrize(
        ("bucket_name", "snapshot_version", "message"),
        [
            ("", "1234567890", "bucket_name is required"),
            ("my-bucket", "", "snapshot_version is required"),
        ],
    )
    def test_rejects_empty_arguments(
        self, mock_s3_client, bucket_name, snapshot_version, message
    ):
        with pytest.raises(ValueError, match=message):
            delete_snapshot(mock_s3_client, bucket_name, snapshot_version)

        mock_s3_client.delete_bucket.assert_not_called()
        mock_s3_client.meta.events.register.assert_not_called()

    def test_rejects_concurrent_calls(self, mock_s3_client):
        other = create_header_injector(
            mock_s3_client, "DeleteBucket", {"X-Tigris-Snapshot-Version": "1"}
        )
        other.register()
        try:
            with pytest.raises(RuntimeError, match="Cannot delete"):
                delete_snapshot(mock_s3_client, "my-bucket", "2")
            mock_s3_client.delete_bucket.assert_not_called()
        finally:
            other.unregister()
