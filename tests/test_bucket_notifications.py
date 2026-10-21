"""Unit tests for object notifications."""

import json
from unittest.mock import patch

import pytest

from tigris_boto3_ext import TigrisAPIError, set_bucket_notifications
from tigris_boto3_ext._transport import TigrisResponse

URL = "https://api.example.com/hooks/tigris"
FILTER = 'WHERE `Event-Type` = "OBJECT_DELETED"'
BEARER = "s3cr3t"
BASIC_AUTH = ("hook", "pw")
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


class TestSetBucketNotifications:
    @patch("tigris_boto3_ext.buckets.notifications.signed_request")
    def test_webhook_with_token_and_filter(self, mock_request, mock_s3_client):
        mock_request.return_value = _ok()

        result = set_bucket_notifications(
            mock_s3_client, "my-bucket", URL, event_filter=FILTER, token=BEARER
        )

        args, headers, body = _sent(mock_request)
        assert args == (mock_s3_client, "PATCH", "my-bucket")
        assert headers == JSON_HEADERS
        assert body == {
            "object_notifications": {
                "enabled": True,
                "web_hook": URL,
                "filter": FILTER,
                "auth": {"token": BEARER},
            }
        }
        assert result == {"name": "my-bucket", "request_id": "req-1", "raw": PATCH_OK}

    @patch("tigris_boto3_ext.buckets.notifications.signed_request")
    def test_basic_auth_paused(self, mock_request, mock_s3_client):
        mock_request.return_value = _ok()
        username, password = BASIC_AUTH

        set_bucket_notifications(
            mock_s3_client,
            "my-bucket",
            URL,
            enabled=False,
            username=username,
            password=password,
        )

        assert _sent(mock_request)[2] == {
            "object_notifications": {
                "enabled": False,
                "web_hook": URL,
                "auth": {"basic_user": username, "basic_pass": password},
            }
        }

    @patch("tigris_boto3_ext.buckets.notifications.signed_request")
    def test_url_alone_sends_enabled_without_filter_or_auth(
        self, mock_request, mock_s3_client
    ):
        mock_request.return_value = _ok()

        set_bucket_notifications(mock_s3_client, "my-bucket", URL)

        assert _sent(mock_request)[2] == {
            "object_notifications": {"enabled": True, "web_hook": URL}
        }

    @patch("tigris_boto3_ext.buckets.notifications.signed_request")
    def test_none_removes_the_configuration(self, mock_request, mock_s3_client):
        mock_request.return_value = _ok()

        set_bucket_notifications(mock_s3_client, "my-bucket", None)

        assert _sent(mock_request)[2] == {"object_notifications": {}}

    @patch("tigris_boto3_ext.buckets.notifications.signed_request")
    def test_empty_response_body(self, mock_request, mock_s3_client):
        mock_request.return_value = TigrisResponse(status_code=200)

        assert set_bucket_notifications(mock_s3_client, "my-bucket", URL)["raw"] == {}

    @pytest.mark.parametrize(
        ("args", "kwargs", "error", "message"),
        [
            (("",), {"url": URL}, ValueError, "bucket_name is required"),
            (("my-bucket", "http://api.example.com/hook"), {}, ValueError, "https://"),
            (("my-bucket", "https://"), {}, ValueError, "https://"),
            (("my-bucket", "https://:443/hook"), {}, ValueError, "https://"),
            (("my-bucket", "api.example.com/hook"), {}, ValueError, "https://"),
            (
                ("my-bucket", URL),
                {"token": "t", "username": "u", "password": "p"},
                ValueError,
                "not both",
            ),
            (("my-bucket", URL), {"username": "u"}, ValueError, "together"),
            (("my-bucket", URL), {"password": "p"}, ValueError, "together"),
            (("my-bucket", URL), {"token": ""}, ValueError, "token must not be empty"),
            (
                ("my-bucket", URL),
                {"username": "u", "password": ""},
                ValueError,
                "must not be empty",
            ),
            (("my-bucket", None), {"token": "t"}, ValueError, "url=None removes"),
            (
                ("my-bucket", None),
                {"event_filter": FILTER},
                ValueError,
                "url=None removes",
            ),
            (
                ("my-bucket", URL),
                {"enabled": "yes"},
                TypeError,
                "enabled must be a bool",
            ),
        ],
    )
    @patch("tigris_boto3_ext.buckets.notifications.signed_request")
    def test_rejects_bad_arguments_before_sending(
        self, mock_request, mock_s3_client, args, kwargs, error, message
    ):
        with pytest.raises(error, match=message):
            set_bucket_notifications(mock_s3_client, *args, **kwargs)

        mock_request.assert_not_called()

    @patch("tigris_boto3_ext.buckets.notifications.signed_request")
    def test_gateway_errors_propagate(self, mock_request, mock_s3_client):
        mock_request.side_effect = TigrisAPIError(
            "Tigris request failed (HTTP 400): InvalidArgument: Invalid filter",
            status_code=400,
            body="{}",
            code="InvalidArgument",
        )

        with pytest.raises(TigrisAPIError) as exc_info:
            set_bucket_notifications(
                mock_s3_client, "my-bucket", URL, event_filter="not a filter ((("
            )

        assert exc_info.value.code == "InvalidArgument"
