"""Unit tests for nested header injectors."""

from botocore.awsrequest import AWSRequest

from tigris_boto3_ext import (
    TigrisSnapshot,
    TigrisSoftDeleteEnabled,
    TigrisSoftDeleteView,
)
from tigris_boto3_ext._internal import _handler_registry, create_header_injector


def _handler_for(client, event_name):
    for call in client.meta.events.register.call_args_list:
        if call[0][0] == event_name:
            return call[0][1]
    return None


class TestNestedInjectors:
    def test_inner_context_overrides_outer_for_the_same_header(
        self, mock_s3_client, mock_request_class
    ):
        with TigrisSoftDeleteEnabled(mock_s3_client):
            with TigrisSoftDeleteEnabled(mock_s3_client, retention_days=30):
                handler = _handler_for(mock_s3_client, "before-sign.s3.CreateBucket")
                request = mock_request_class()
                handler(request)
                assert request.headers == {"X-Tigris-Soft-Delete": "30"}

            request = mock_request_class()
            handler(request)
            assert request.headers == {"X-Tigris-Soft-Delete": "true"}

    def test_different_contexts_on_one_operation_both_apply(
        self, mock_s3_client, mock_request_class
    ):
        with (
            TigrisSnapshot(mock_s3_client, "my-bucket", snapshot_version="123"),
            TigrisSoftDeleteView(mock_s3_client),
        ):
            handler = _handler_for(mock_s3_client, "before-sign.s3.ListObjectsV2")
            request = mock_request_class()
            handler(request)
            assert request.headers == {
                "X-Tigris-Snapshot-Version": "123",
                "X-Tigris-Soft-Delete": "true",
            }

    def test_nested_contexts_do_not_duplicate_headers_on_a_real_request(
        self, mock_s3_client
    ):
        with (
            TigrisSoftDeleteEnabled(mock_s3_client),
            TigrisSoftDeleteEnabled(mock_s3_client, retention_days=30),
        ):
            handler = _handler_for(mock_s3_client, "before-sign.s3.CreateBucket")
            request = AWSRequest(method="PUT", url="https://t3.storage.dev/my-bucket")
            handler(request)

            assert request.headers.get_all("X-Tigris-Soft-Delete") == ["30"]


class TestUnregisterEdgeCases:
    def test_unregister_without_register_is_a_noop(self, mock_s3_client):
        injector = create_header_injector(
            mock_s3_client, "CreateBucket", {"X-Test": "1"}
        )

        injector.unregister()

        mock_s3_client.meta.events.unregister.assert_not_called()

    def test_unregistering_a_stranger_keeps_the_active_handler(self, mock_s3_client):
        active = create_header_injector(mock_s3_client, "CreateBucket", {"X-Test": "1"})
        stranger = create_header_injector(
            mock_s3_client, "CreateBucket", {"X-Test": "2"}
        )
        active.register()
        try:
            stranger.unregister()

            mock_s3_client.meta.events.unregister.assert_not_called()
            key = (id(mock_s3_client), "before-sign.s3.CreateBucket")
            assert key in _handler_registry
        finally:
            active.unregister()
