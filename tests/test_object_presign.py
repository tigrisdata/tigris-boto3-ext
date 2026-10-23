"""Unit tests for presigned URLs from the presign endpoint."""

import json
from unittest.mock import MagicMock, patch

import pytest

from tigris_boto3_ext import TigrisAPIError, presign
from tigris_boto3_ext._transport import TigrisResponse

URL = "https://assets.t3.tigrisfiles.io/report.pdf?X-Amz-Algorithm=AWS4-HMAC-SHA256"
JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}


@pytest.fixture
def client(mock_s3_client):
    frozen = MagicMock()
    frozen.access_key = "tid_example"
    mock_s3_client._request_signer._credentials.get_frozen_credentials.return_value = (
        frozen
    )
    return mock_s3_client


def _ok(**extra: object) -> TigrisResponse:
    payload = {
        "bucket": "assets",
        "key": "report.pdf",
        "key_id": "tid_example",
        "key_secret": "should-never-be-returned",
        "type": "get",
        "url": URL,
        "custom_domain_url": "",
        **extra,
    }
    return TigrisResponse(status_code=200, body=json.dumps(payload).encode())


def _sent(mock_request):
    args, kwargs = mock_request.call_args
    return args, kwargs, json.loads(kwargs["body"])


class TestPresign:
    @patch("tigris_boto3_ext.objects.presign.signed_request")
    def test_get_url(self, mock_request, client):
        mock_request.return_value = _ok()

        result = presign(client, "assets", "report.pdf", expires_in=600)

        args, kwargs, body = _sent(mock_request)
        assert args == (client, "POST", "")
        assert kwargs["query"] == {"func": "presign"}
        assert kwargs["headers"] == JSON_HEADERS
        assert body == {
            "bucket": "assets",
            "key": "report.pdf",
            "key_id": "tid_example",
            "type": "get",
            "expires_in": 600,
        }
        assert result == {
            "url": URL,
            "custom_domain_url": None,
            "operation": "get",
            "expires_in": 600,
            "version_id": None,
        }
        client.head_object.assert_not_called()

    @patch("tigris_boto3_ext.objects.presign.signed_request")
    def test_put_url_with_default_expiry_and_custom_domain(self, mock_request, client):
        mock_request.return_value = _ok(
            type="put", custom_domain_url="https://files.example.com/report.pdf?X-Amz"
        )

        result = presign(client, "assets", "report.pdf", "put")

        assert _sent(mock_request)[2]["type"] == "put"
        assert _sent(mock_request)[2]["expires_in"] == 3600
        assert result["operation"] == "put"
        assert result["expires_in"] == 3600
        assert (
            result["custom_domain_url"] == "https://files.example.com/report.pdf?X-Amz"
        )

    @patch("tigris_boto3_ext.objects.presign.signed_request")
    def test_snapshot_version_resolves_to_the_object_version(
        self, mock_request, client, mock_request_class
    ):
        mock_request.return_value = _ok()
        client.head_object.return_value = {"VersionId": "1792757953358921503"}

        result = presign(
            client, "assets", "report.pdf", snapshot_version="1792757954482613190"
        )

        client.head_object.assert_called_once_with(Bucket="assets", Key="report.pdf")
        handlers = {
            call[0][0]: call[0][1]
            for call in client.meta.events.register.call_args_list
        }
        request = mock_request_class()
        handlers["before-sign.s3.HeadObject"](request)
        assert request.headers["X-Tigris-Snapshot-Version"] == "1792757954482613190"
        assert _sent(mock_request)[2]["version_id"] == "1792757953358921503"
        assert result["version_id"] == "1792757953358921503"

    @patch("tigris_boto3_ext.objects.presign.signed_request")
    def test_snapshot_without_a_version_id_is_an_error(self, mock_request, client):
        client.head_object.return_value = {"ContentLength": 1}

        with pytest.raises(ValueError, match="no version"):
            presign(
                client, "assets", "report.pdf", snapshot_version="1792757954482613190"
            )

        mock_request.assert_not_called()

    @pytest.mark.parametrize(
        ("args", "kwargs", "error", "message"),
        [
            (("", "report.pdf"), {}, ValueError, "bucket_name is required"),
            (("assets", ""), {}, ValueError, "key is required"),
            (("assets", "report.pdf", "delete"), {}, ValueError, "operation must be"),
            (("assets", "report.pdf"), {"expires_in": 0}, ValueError, "at least 1"),
            (
                ("assets", "report.pdf"),
                {"expires_in": "600"},
                TypeError,
                "whole number",
            ),
            (("assets", "report.pdf"), {"expires_in": True}, TypeError, "whole number"),
            (
                ("assets", "report.pdf", "put"),
                {"snapshot_version": "1792757954482613190"},
                ValueError,
                "snapshot_version only applies",
            ),
            (
                ("assets", "report.pdf"),
                {"snapshot_version": ""},
                ValueError,
                "must not be empty",
            ),
        ],
    )
    @patch("tigris_boto3_ext.objects.presign.signed_request")
    def test_rejects_bad_arguments_before_sending(
        self, mock_request, client, args, kwargs, error, message
    ):
        with pytest.raises(error, match=message):
            presign(client, *args, **kwargs)

        mock_request.assert_not_called()
        client.head_object.assert_not_called()

    @patch("tigris_boto3_ext.objects.presign.signed_request")
    def test_gateway_errors_propagate(self, mock_request, client):
        mock_request.side_effect = TigrisAPIError(
            "Tigris request failed (HTTP 400): InvalidRequest: Invalid Request",
            status_code=400,
            body="{}",
            code="InvalidRequest",
        )

        with pytest.raises(TigrisAPIError) as exc_info:
            presign(client, "assets", "report.pdf")

        assert exc_info.value.code == "InvalidRequest"
