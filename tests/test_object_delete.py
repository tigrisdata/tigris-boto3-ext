"""Unit tests for object deletion: the soft-delete view, listing, purge and restore."""

import pytest

from tigris_boto3_ext import (
    TigrisSoftDeleteView,
    list_deleted_object_versions,
    list_deleted_objects,
    purge_deleted_object,
    restore_deleted_object,
    with_soft_delete_view,
)
from tigris_boto3_ext._internal import create_header_injector

RESTORE_EVENT = "before-sign.s3.RestoreObject"


class TestRestoreDeletedObjectHelper:
    def test_restores_a_specific_version(self, mock_s3_client, mock_request_class):
        mock_s3_client.restore_object.return_value = {"ResponseMetadata": {}}

        result = restore_deleted_object(mock_s3_client, "my-bucket", "file.txt", "123")

        assert result == {"ResponseMetadata": {}}
        mock_s3_client.restore_object.assert_called_once_with(
            Bucket="my-bucket", Key="file.txt"
        )
        event_name, handler = mock_s3_client.meta.events.register.call_args[0]
        assert event_name == RESTORE_EVENT
        request = mock_request_class()
        handler(request)
        assert request.headers == {
            "X-Tigris-Restore-Type": "soft-delete",
            "X-Tigris-Restore-Version": "123",
        }
        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_omits_version_header_without_a_version(
        self, mock_s3_client, mock_request_class
    ):
        restore_deleted_object(mock_s3_client, "my-bucket", "file.txt")

        _, handler = mock_s3_client.meta.events.register.call_args[0]
        request = mock_request_class()
        handler(request)
        assert request.headers == {"X-Tigris-Restore-Type": "soft-delete"}

    def test_unregisters_when_restore_object_raises(self, mock_s3_client):
        mock_s3_client.restore_object.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            restore_deleted_object(mock_s3_client, "my-bucket", "file.txt", "123")

        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_refuses_to_overlap_on_the_same_client(self, mock_s3_client):
        other = create_header_injector(
            mock_s3_client, "RestoreObject", {"X-Tigris-Restore-Type": "soft-delete"}
        )
        other.register()
        try:
            with pytest.raises(RuntimeError, match="already active"):
                restore_deleted_object(mock_s3_client, "my-bucket", "file.txt", "123")
            mock_s3_client.restore_object.assert_not_called()
        finally:
            other.unregister()

    @pytest.mark.parametrize(
        ("bucket_name", "key", "message"),
        [
            ("", "file.txt", "bucket_name is required"),
            ("my-bucket", "", "key is required"),
        ],
    )
    def test_rejects_empty_arguments(self, mock_s3_client, bucket_name, key, message):
        with pytest.raises(ValueError, match=message):
            restore_deleted_object(mock_s3_client, bucket_name, key)

        mock_s3_client.restore_object.assert_not_called()
        mock_s3_client.meta.events.register.assert_not_called()

    def test_rejects_an_empty_version_id(self, mock_s3_client):
        with pytest.raises(ValueError, match="version_id must not be empty"):
            restore_deleted_object(mock_s3_client, "my-bucket", "file.txt", "")

        mock_s3_client.restore_object.assert_not_called()
        mock_s3_client.meta.events.register.assert_not_called()


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


class TestListDeletedObjectsHelpers:
    def test_list_deleted_objects_uses_the_soft_delete_view(self, mock_s3_client):
        mock_s3_client.list_objects_v2.return_value = {"Contents": []}

        result = list_deleted_objects(mock_s3_client, "my-bucket", Prefix="logs/")

        assert result == {"Contents": []}
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="my-bucket", Prefix="logs/"
        )
        calls = mock_s3_client.meta.events.register.call_args_list
        assert [call[0][0] for call in calls] == SOFT_DELETE_EVENTS
        assert mock_s3_client.meta.events.unregister.call_count == 3

    def test_list_deleted_object_versions_uses_the_soft_delete_view(
        self, mock_s3_client
    ):
        mock_s3_client.list_object_versions.return_value = {"Versions": []}

        result = list_deleted_object_versions(
            mock_s3_client, "my-bucket", KeyMarker="k", VersionIdMarker="v"
        )

        assert result == {"Versions": []}
        mock_s3_client.list_object_versions.assert_called_once_with(
            Bucket="my-bucket", KeyMarker="k", VersionIdMarker="v"
        )
        calls = mock_s3_client.meta.events.register.call_args_list
        assert [call[0][0] for call in calls] == SOFT_DELETE_EVENTS
        assert mock_s3_client.meta.events.unregister.call_count == 3
