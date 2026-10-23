"""Unit tests for bucket data migration (shadow bucket)."""

import json
from unittest.mock import patch

import pytest

from tigris_boto3_ext import TigrisAPIError, set_bucket_migration
from tigris_boto3_ext._transport import TigrisResponse

SOURCE = {
    "endpoint": "https://s3.amazonaws.com",
    "region": "us-east-1",
    "access_key": "AKIAEXAMPLE",
    "secret_key": "example",
}
PATCH_OK = {"Bucket": "my-bucket", "Update": "success"}
JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}


def _ok():
    return TigrisResponse(
        status_code=200,
        headers={"x-amz-request-id": "req-1"},
        body=json.dumps(PATCH_OK).encode(),
    )


def _sent(mock_request):
    args, kwargs = mock_request.call_args
    return args, kwargs["headers"], json.loads(kwargs["body"])


class TestSetBucketMigration:
    @patch("tigris_boto3_ext.buckets.migration.signed_request")
    def test_configures_the_shadow_bucket(self, mock_request, mock_s3_client):
        mock_request.return_value = _ok()

        result = set_bucket_migration(
            mock_s3_client, "my-bucket", "legacy", write_through=True, **SOURCE
        )

        args, headers, body = _sent(mock_request)
        assert args == (mock_s3_client, "PATCH", "my-bucket")
        assert headers == JSON_HEADERS
        assert body == {
            "shadow_bucket": {
                "name": "legacy",
                "endpoint": "https://s3.amazonaws.com",
                "region": "us-east-1",
                "access_key": "AKIAEXAMPLE",
                "secret_key": "example",
                "write_through": True,
            }
        }
        assert result == {"name": "my-bucket", "request_id": "req-1", "raw": PATCH_OK}

    @patch("tigris_boto3_ext.buckets.migration.signed_request")
    def test_write_through_defaults_to_false(self, mock_request, mock_s3_client):
        mock_request.return_value = _ok()

        set_bucket_migration(mock_s3_client, "my-bucket", "legacy", **SOURCE)

        assert _sent(mock_request)[2]["shadow_bucket"]["write_through"] is False

    @patch("tigris_boto3_ext.buckets.migration.signed_request")
    def test_none_removes_the_configuration(self, mock_request, mock_s3_client):
        mock_request.return_value = _ok()

        set_bucket_migration(mock_s3_client, "my-bucket", None)

        assert _sent(mock_request)[2] == {"shadow_bucket": {}}

    @patch("tigris_boto3_ext.buckets.migration.signed_request")
    def test_empty_response_body(self, mock_request, mock_s3_client):
        mock_request.return_value = TigrisResponse(status_code=200)

        assert set_bucket_migration(mock_s3_client, "my-bucket", None)["raw"] == {}

    @pytest.mark.parametrize(
        ("args", "kwargs", "error", "message"),
        [
            (("", "legacy"), SOURCE, ValueError, "bucket_name is required"),
            (
                ("my-bucket", "legacy"),
                {},
                ValueError,
                "needs endpoint, region, access_key, secret_key",
            ),
            (
                ("my-bucket", "legacy"),
                {**SOURCE, "secret_key": None},
                ValueError,
                "needs secret_key",
            ),
            (("my-bucket", ""), SOURCE, ValueError, "needs source_bucket"),
            (
                ("my-bucket", "legacy"),
                {**SOURCE, "endpoint": "http://s3.amazonaws.com"},
                ValueError,
                "endpoint must be an https://",
            ),
            (
                ("my-bucket", "legacy"),
                {**SOURCE, "endpoint": "s3.amazonaws.com"},
                ValueError,
                "endpoint must be an https://",
            ),
            (
                ("my-bucket", None),
                {"region": "auto"},
                ValueError,
                "source_bucket=None removes",
            ),
            (
                ("my-bucket", None),
                {"write_through": True},
                ValueError,
                "source_bucket=None removes",
            ),
            (
                ("my-bucket", "legacy"),
                {**SOURCE, "write_through": "yes"},
                TypeError,
                "write_through must be a bool",
            ),
        ],
    )
    @patch("tigris_boto3_ext.buckets.migration.signed_request")
    def test_rejects_bad_arguments_before_sending(
        self, mock_request, mock_s3_client, args, kwargs, error, message
    ):
        with pytest.raises(error, match=message):
            set_bucket_migration(mock_s3_client, *args, **kwargs)

        mock_request.assert_not_called()

    @patch("tigris_boto3_ext.buckets.migration.signed_request")
    def test_gateway_errors_propagate(self, mock_request, mock_s3_client):
        mock_request.side_effect = TigrisAPIError(
            "Tigris request failed (HTTP 403): InvalidAccessKeyId: Shadow bucket: ...",
            status_code=403,
            body="{}",
            code="InvalidAccessKeyId",
        )

        with pytest.raises(TigrisAPIError) as exc_info:
            set_bucket_migration(mock_s3_client, "my-bucket", "legacy", **SOURCE)

        assert exc_info.value.status_code == 403
        assert exc_info.value.code == "InvalidAccessKeyId"
