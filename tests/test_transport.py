"""Unit tests for the signed request transport and TigrisAPIError."""

import json
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import NoCredentialsError

from tigris_boto3_ext import BundleError, TigrisAPIError
from tigris_boto3_ext._transport import TigrisResponse, signed_request


class FakeResponse:
    """The parts of botocore's AWSResponse that signed_request reads."""

    def __init__(self, status_code=200, body=b"", headers=None, unreadable=False):
        self.status_code = status_code
        self.headers = headers or {}
        self.raw = MagicMock()
        self.raw.read.return_value = body
        self._body = body
        self._unreadable = unreadable

    @property
    def content(self):
        if self._unreadable:
            msg = "connection reset"
            raise OSError(msg)
        return self._body


@pytest.fixture
def rest_client(mock_s3_client):
    mock_s3_client.meta.endpoint_url = "https://t3.storage.dev"
    mock_s3_client.meta.region_name = "auto"
    return mock_s3_client


@pytest.fixture
def sigv4():
    with patch("tigris_boto3_ext._transport.SigV4Auth") as mock_sigv4:
        yield mock_sigv4


@pytest.fixture
def transport(rest_client, sigv4):
    """The client's own HTTP session, which signed_request sends through."""
    return rest_client._endpoint.http_session.send


class TestSignedRequest:
    def test_builds_signs_and_sends_through_the_client_session(
        self, transport, sigv4, rest_client
    ):
        transport.return_value = FakeResponse(
            200, b'{"name": "my-bucket"}', {"Content-Type": "application/json"}
        )

        result = signed_request(
            rest_client,
            "GET",
            "my-bucket",
            query={"metadata": None, "with-size": "true"},
            headers={"Accept": "application/json"},
        )

        sigv4.assert_called_once()
        assert sigv4.call_args[0][1:] == ("s3", "auto")
        transport.assert_called_once()
        prepared = transport.call_args[0][0]
        assert prepared.method == "GET"
        assert (
            prepared.url == "https://t3.storage.dev/my-bucket?metadata&with-size=true"
        )
        assert prepared.headers["Accept"] == "application/json"
        assert "X-Amz-Content-Sha256" in prepared.headers
        assert prepared.stream_output is False
        assert isinstance(result, TigrisResponse)
        assert result.status_code == 200
        assert result.headers == {"content-type": "application/json"}
        assert result.json() == {"name": "my-bucket"}

    def test_encodes_string_bodies_and_quotes_query_values(
        self, transport, rest_client
    ):
        transport.return_value = FakeResponse(200)

        signed_request(
            rest_client,
            "POST",
            "/my-bucket",
            query={"continuation-token": "a b/c"},
            body='{"keys": []}',
        )

        prepared = transport.call_args[0][0]
        assert (
            prepared.url
            == "https://t3.storage.dev/my-bucket?continuation-token=a%20b%2Fc"
        )
        assert prepared.body == b'{"keys": []}'

    def test_string_query_is_passed_through(self, transport, rest_client):
        transport.return_value = FakeResponse(200)

        signed_request(rest_client, "GET", "", query="IncludeStats=true")

        assert (
            transport.call_args[0][0].url == "https://t3.storage.dev/?IncludeStats=true"
        )

    def test_stream_returns_the_raw_response_unread(self, transport, rest_client):
        response = FakeResponse(200)
        transport.return_value = response

        result = signed_request(
            rest_client, "POST", "my-bucket", query="bundle", stream=True
        )

        assert result is response.raw
        assert transport.call_args[0][0].stream_output is True
        response.raw.read.assert_not_called()

    def test_requires_credentials(self, rest_client):
        rest_client._request_signer._credentials = None

        with pytest.raises(NoCredentialsError):
            signed_request(rest_client, "GET", "my-bucket")


class TestSignedRequestErrors:
    def test_json_error_document(self, transport, rest_client):
        body = json.dumps(
            {
                "Code": "NoSuchBucket",
                "Message": "The specified bucket does not exist",
                "RequestId": "1791986200643024440",
            }
        ).encode()
        response = FakeResponse(404, body, {"Content-Type": "application/json"})
        transport.return_value = response

        with pytest.raises(TigrisAPIError) as exc_info:
            signed_request(rest_client, "GET", "missing", query="metadata")

        error = exc_info.value
        assert error.status_code == 404
        assert error.code == "NoSuchBucket"
        assert error.message == (
            "Tigris request failed (HTTP 404): NoSuchBucket: The specified bucket does not exist"
        )
        assert str(error) == error.message
        assert error.request_id == "1791986200643024440"
        assert error.body == body.decode()
        response.raw.close.assert_called_once()

    def test_xml_error_document_is_unescaped(self, transport, rest_client):
        body = (
            b'<?xml version="1.0" encoding="UTF-8"?>\n<Error><Code>AccessDenied</Code>'
            b"<Message>Access &amp; Denied &lt;here&gt;</Message>"
            b"<RequestId>abc123</RequestId></Error>"
        )
        transport.return_value = FakeResponse(403, body)

        with pytest.raises(
            TigrisAPIError, match="AccessDenied: Access & Denied <here>"
        ) as exc_info:
            signed_request(rest_client, "GET", "secret-bucket", query="metadata")

        assert exc_info.value.code == "AccessDenied"
        assert exc_info.value.request_id == "abc123"

    def test_plain_body_falls_back_to_the_header_request_id(
        self, transport, rest_client
    ):
        transport.return_value = FakeResponse(
            502, b"upstream unavailable", {"X-Amz-Request-Id": "req-9"}
        )

        with pytest.raises(
            TigrisAPIError, match=r"HTTP 502.*upstream unavailable"
        ) as exc_info:
            signed_request(rest_client, "GET", "my-bucket")

        assert exc_info.value.code is None
        assert exc_info.value.message.endswith("upstream unavailable")
        assert exc_info.value.request_id == "req-9"

    def test_malformed_json_error_body_is_kept_raw(self, transport, rest_client):
        transport.return_value = FakeResponse(500, b"{not json")

        with pytest.raises(TigrisAPIError, match=r"HTTP 500.*\{not json") as exc_info:
            signed_request(rest_client, "GET", "my-bucket")

        assert exc_info.value.code is None

    def test_streamed_error_is_read_and_closed(self, transport, rest_client):
        response = FakeResponse(400, b'{"Code": "InvalidRequest", "Message": "bad"}')
        transport.return_value = response

        with pytest.raises(TigrisAPIError) as exc_info:
            signed_request(
                rest_client, "POST", "my-bucket", query="bundle", stream=True
            )

        response.raw.close.assert_called_once()
        assert exc_info.value.code == "InvalidRequest"

    def test_unreadable_error_body_still_raises(self, transport, rest_client):
        response = FakeResponse(500, unreadable=True)
        transport.return_value = response

        with pytest.raises(TigrisAPIError, match="HTTP 500") as exc_info:
            signed_request(rest_client, "GET", "my-bucket")

        assert exc_info.value.body == ""
        response.raw.close.assert_called_once()

    def test_bundle_error_keeps_the_parsed_fields(self):
        error = BundleError(
            "boom", status_code=400, body="bad", code="X", request_id="r1"
        )

        assert isinstance(error, TigrisAPIError)
        assert (error.status_code, error.body, error.code, error.request_id) == (
            400,
            "bad",
            "X",
            "r1",
        )
        assert BundleError("boom", 400, "bad").code is None
