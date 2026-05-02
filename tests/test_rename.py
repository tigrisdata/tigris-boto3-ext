"""Unit tests for rename functionality."""

from tigris_boto3_ext import TigrisRename, rename_object, with_rename
from tigris_boto3_ext._internal import _handler_registry


def _captured_handler(client, event_name="before-sign.s3.CopyObject"):
    """Look up the shared handler that was registered for `event_name`."""
    key = (id(client), event_name)
    entry = _handler_registry.get(key)
    return entry[0] if entry else None


class TestTigrisRenameContextManager:
    def test_registers_and_unregisters_handler(self, mock_s3_client):
        with TigrisRename(mock_s3_client):
            mock_s3_client.meta.events.register.assert_called_once()
            event_name = mock_s3_client.meta.events.register.call_args[0][0]
            assert event_name == "before-sign.s3.CopyObject"

        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_handler_injects_rename_header(self, mock_s3_client, mock_request_class):
        with TigrisRename(mock_s3_client):
            handler = _captured_handler(mock_s3_client)
            assert handler is not None
            request = mock_request_class()
            handler(request)
            assert request.headers["X-Tigris-Rename"] == "true"

    def test_handler_unregistered_after_exit(self, mock_s3_client):
        with TigrisRename(mock_s3_client):
            pass
        key = (id(mock_s3_client), "before-sign.s3.CopyObject")
        assert key not in _handler_registry

    def test_unregisters_on_exception(self, mock_s3_client):
        try:
            with TigrisRename(mock_s3_client):
                raise RuntimeError("boom")
        except RuntimeError:
            pass

        mock_s3_client.meta.events.unregister.assert_called_once()
        key = (id(mock_s3_client), "before-sign.s3.CopyObject")
        assert key not in _handler_registry


class TestWithRenameDecorator:
    def test_decorator_wraps_function_in_rename_context(self, mock_s3_client):
        @with_rename
        def do_rename(client, src, dst):
            client.copy_object(
                Bucket="b", CopySource=f"b/{src}", Key=dst,
            )
            return "done"

        result = do_rename(mock_s3_client, "old.txt", "new.txt")

        assert result == "done"
        mock_s3_client.copy_object.assert_called_once_with(
            Bucket="b", CopySource="b/old.txt", Key="new.txt",
        )
        # Handler must be torn down once the function returns.
        mock_s3_client.meta.events.unregister.assert_called_once()


class TestRenameObjectHelper:
    def test_calls_copy_object_with_correct_args(self, mock_s3_client):
        mock_s3_client.copy_object.return_value = {"ResponseMetadata": {}}

        result = rename_object(
            mock_s3_client, "my-bucket", "old-name.txt", "new-name.txt"
        )

        assert result == {"ResponseMetadata": {}}
        mock_s3_client.copy_object.assert_called_once_with(
            Bucket="my-bucket",
            CopySource="my-bucket/old-name.txt",
            Key="new-name.txt",
        )
        # Header injector must be unregistered when the helper returns.
        mock_s3_client.meta.events.unregister.assert_called_once()

    def test_passes_through_extra_kwargs(self, mock_s3_client):
        rename_object(
            mock_s3_client,
            "my-bucket",
            "old.txt",
            "new.txt",
            MetadataDirective="REPLACE",
            Metadata={"foo": "bar"},
        )

        mock_s3_client.copy_object.assert_called_once_with(
            Bucket="my-bucket",
            CopySource="my-bucket/old.txt",
            Key="new.txt",
            MetadataDirective="REPLACE",
            Metadata={"foo": "bar"},
        )

    def test_handles_keys_with_slashes(self, mock_s3_client):
        rename_object(
            mock_s3_client,
            "my-bucket",
            "dir/sub/old.txt",
            "dir/sub/new.txt",
        )

        mock_s3_client.copy_object.assert_called_once_with(
            Bucket="my-bucket",
            CopySource="my-bucket/dir/sub/old.txt",
            Key="dir/sub/new.txt",
        )
