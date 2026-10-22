"""Unit tests for object migration: list source, scheduling and the migrated check."""

import pytest

from tigris_boto3_ext import (
    TigrisListSource,
    is_object_migrated,
    list_objects_from_source,
    migrate_object,
    with_list_source,
)

LIST_EVENT = "before-sign.s3.ListObjectsV2"
HEAD_EVENT = "before-sign.s3.HeadObject"


def _head_response(read_source=None):
    headers = {"x-tigris-regions": "fra"}
    if read_source is not None:
        headers["x-tigris-read-source"] = read_source
    return {"ContentLength": 1, "ResponseMetadata": {"HTTPHeaders": headers}}


class TestTigrisListSource:
    @pytest.mark.parametrize("source", ["tigris", "shadow"])
    def test_registers_the_list_source_header(
        self, mock_s3_client, mock_request_class, source
    ):
        with TigrisListSource(mock_s3_client, source):
            event_name, handler = mock_s3_client.meta.events.register.call_args[0]
            assert event_name == LIST_EVENT
            request = mock_request_class()
            handler(request)
            assert request.headers == {"X-Tigris-List-Source": source}

        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_rejects_other_sources(self, mock_s3_client):
        with pytest.raises(ValueError, match="source must be 'tigris' or 'shadow'"):
            TigrisListSource(mock_s3_client, "both")

        mock_s3_client.meta.events.register.assert_not_called()


class TestWithListSourceDecorator:
    def test_wraps_function_in_list_source(self, mock_s3_client, mock_request_class):
        @with_list_source("shadow")
        def registered_handler(client, bucket):
            return client.meta.events.register.call_args[0]

        event_name, handler = registered_handler(mock_s3_client, "my-bucket")

        assert event_name == LIST_EVENT
        request = mock_request_class()
        handler(request)
        assert request.headers == {"X-Tigris-List-Source": "shadow"}
        mock_s3_client.meta.events.unregister.assert_called_once()


class TestListObjectsFromSource:
    def test_lists_inside_the_source_context(self, mock_s3_client):
        mock_s3_client.list_objects_v2.return_value = {"Contents": []}

        result = list_objects_from_source(
            mock_s3_client, "my-bucket", "shadow", Prefix="images/", MaxKeys=10
        )

        assert result == {"Contents": []}
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="my-bucket", Prefix="images/", MaxKeys=10
        )
        assert mock_s3_client.meta.events.register.call_args[0][0] == LIST_EVENT
        mock_s3_client.meta.events.unregister.assert_called_once()

    @pytest.mark.parametrize(
        ("bucket_name", "source", "message"),
        [
            ("", "shadow", "bucket_name is required"),
            ("my-bucket", "mirror", "source must be"),
        ],
    )
    def test_rejects_bad_arguments(self, mock_s3_client, bucket_name, source, message):
        with pytest.raises(ValueError, match=message):
            list_objects_from_source(mock_s3_client, bucket_name, source)

        mock_s3_client.list_objects_v2.assert_not_called()


class TestMigrateObject:
    def test_heads_with_the_schedule_header(self, mock_s3_client, mock_request_class):
        mock_s3_client.head_object.return_value = _head_response("cache")

        result = migrate_object(mock_s3_client, "my-bucket", "file.txt", VersionId="v1")

        assert result == _head_response("cache")
        mock_s3_client.head_object.assert_called_once_with(
            Bucket="my-bucket", Key="file.txt", VersionId="v1"
        )
        event_name, handler = mock_s3_client.meta.events.register.call_args[0]
        assert event_name == HEAD_EVENT
        request = mock_request_class()
        handler(request)
        assert request.headers == {"X-Tigris-Schedule-Migration": "true"}
        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_unregisters_when_head_object_raises(self, mock_s3_client):
        mock_s3_client.head_object.side_effect = RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            migrate_object(mock_s3_client, "my-bucket", "file.txt")

        mock_s3_client.meta.events.unregister.assert_called_once()

    @pytest.mark.parametrize(
        ("bucket_name", "key", "message"),
        [
            ("", "file.txt", "bucket_name is required"),
            ("my-bucket", "", "key is required"),
        ],
    )
    def test_rejects_empty_arguments(self, mock_s3_client, bucket_name, key, message):
        with pytest.raises(ValueError, match=message):
            migrate_object(mock_s3_client, bucket_name, key)

        mock_s3_client.head_object.assert_not_called()
        mock_s3_client.meta.events.register.assert_not_called()


class TestIsObjectMigrated:
    @pytest.mark.parametrize(
        ("read_source", "expected"),
        [
            ("block_shadow", False),
            ("BLOCK_SHADOW", False),
            ("cache", True),
            ("tigris", True),
            (None, True),
        ],
    )
    def test_reads_the_read_source_header(self, mock_s3_client, read_source, expected):
        mock_s3_client.head_object.return_value = _head_response(read_source)

        assert is_object_migrated(mock_s3_client, "my-bucket", "file.txt") is expected

        mock_s3_client.head_object.assert_called_once_with(
            Bucket="my-bucket", Key="file.txt"
        )
        mock_s3_client.meta.events.register.assert_not_called()

    def test_passes_extra_arguments_through(self, mock_s3_client):
        mock_s3_client.head_object.return_value = _head_response("cache")

        is_object_migrated(mock_s3_client, "my-bucket", "file.txt", VersionId="v1")

        mock_s3_client.head_object.assert_called_once_with(
            Bucket="my-bucket", Key="file.txt", VersionId="v1"
        )

    def test_rejects_empty_arguments(self, mock_s3_client):
        with pytest.raises(ValueError, match="key is required"):
            is_object_migrated(mock_s3_client, "my-bucket", "")

        mock_s3_client.head_object.assert_not_called()
