"""Unit tests for the Tigris IAM HTTP transport."""

import json
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs

import pytest

from tigris_boto3_ext._iam import (
    DEFAULT_IAM_ENDPOINT,
    TigrisIAMError,
    create_access_key_with_buckets_role,
    delete_access_key,
)


@pytest.fixture
def mock_s3_client():
    client = MagicMock()
    client.meta.endpoint_url = "https://t3.storage.dev"
    client.meta.region_name = "auto"
    client._request_signer._credentials.get_frozen_credentials.return_value = (
        MagicMock()
    )
    return client


def _ok_response(body: bytes):
    response = MagicMock()
    response.status = 200
    response.data = body
    return response


class TestCreateAccessKey:
    @patch("tigris_boto3_ext._iam._iam_pool")
    @patch("tigris_boto3_ext._iam.SigV4Auth")
    def test_sends_create_request_with_buckets_role(
        self, mock_sigv4, mock_pool, mock_s3_client
    ):
        mock_pool.urlopen.return_value = _ok_response(
            json.dumps(
                {
                    "CreateAccessKeyResult": {
                        "AccessKey": {
                            "AccessKeyId": "AKIAEXAMPLE",
                            "SecretAccessKey": "secret",
                            "UserName": "ws-1-key",
                        }
                    }
                }
            ).encode()
        )

        result = create_access_key_with_buckets_role(
            mock_s3_client,
            "ws-1-key",
            [{"bucket": "ws-1", "role": "Editor"}],
        )

        assert result == {
            "access_key_id": "AKIAEXAMPLE",
            "secret_access_key": "secret",
            "name": "ws-1-key",
        }

        args, kwargs = mock_pool.urlopen.call_args
        assert args[0] == "POST"
        assert "/?Action=CreateAccessKeyWithBucketsRole" in args[1]
        assert DEFAULT_IAM_ENDPOINT in args[1]

        sent = kwargs["body"]
        if isinstance(sent, bytes):
            sent = sent.decode()
        params = parse_qs(sent)
        body = json.loads(params["Req"][0])
        assert body["name"] == "ws-1-key"
        assert body["buckets_role"] == [{"bucket": "ws-1", "role": "Editor"}]
        assert "req_uuid" in body

        # SigV4 needs the body hash in canonical headers.
        assert "X-Amz-Content-Sha256" in kwargs["headers"]
        assert (
            kwargs["headers"]["Content-Type"]
            == "application/x-www-form-urlencoded"
        )
        # SigV4 service must be "iam".
        service_arg = mock_sigv4.call_args.args[1]
        assert service_arg == "iam"

    @patch("tigris_boto3_ext._iam._iam_pool")
    @patch("tigris_boto3_ext._iam.SigV4Auth")
    def test_parses_xml_response(self, _sigv4, mock_pool, mock_s3_client):
        xml = (
            '<?xml version="1.0"?>'
            '<CreateAccessKeyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">'
            "<CreateAccessKeyResult>"
            "<AccessKey>"
            "<UserName>fork-key</UserName>"
            "<AccessKeyId>AKIAXMLKEY</AccessKeyId>"
            "<SecretAccessKey>xmlsecret</SecretAccessKey>"
            "<Status>Active</Status>"
            "</AccessKey>"
            "</CreateAccessKeyResult>"
            "</CreateAccessKeyResponse>"
        )
        mock_pool.urlopen.return_value = _ok_response(xml.encode())

        result = create_access_key_with_buckets_role(
            mock_s3_client, "fork-key", []
        )
        assert result["access_key_id"] == "AKIAXMLKEY"
        assert result["secret_access_key"] == "xmlsecret"
        assert result["name"] == "fork-key"

    @patch("tigris_boto3_ext._iam._iam_pool")
    @patch("tigris_boto3_ext._iam.SigV4Auth")
    def test_403_raises_iam_error(self, _sigv4, mock_pool, mock_s3_client):
        response = MagicMock()
        response.status = 403
        response.data = b"<Error><Message>denied</Message></Error>"
        mock_pool.urlopen.return_value = response

        with pytest.raises(TigrisIAMError) as exc_info:
            create_access_key_with_buckets_role(mock_s3_client, "k", [])
        assert exc_info.value.status_code == 403
        assert "denied" in exc_info.value.body

    @patch("tigris_boto3_ext._iam._iam_pool")
    @patch("tigris_boto3_ext._iam.SigV4Auth")
    def test_unexpected_response_raises(
        self, _sigv4, mock_pool, mock_s3_client
    ):
        mock_pool.urlopen.return_value = _ok_response(b'{"unexpected": true}')
        with pytest.raises(RuntimeError, match="unexpected response"):
            create_access_key_with_buckets_role(mock_s3_client, "k", [])

    @patch.dict("os.environ", {"TIGRIS_IAM_ENDPOINT": "https://iam.test"})
    @patch("tigris_boto3_ext._iam._iam_pool")
    @patch("tigris_boto3_ext._iam.SigV4Auth")
    def test_endpoint_override_via_env(
        self, _sigv4, mock_pool, mock_s3_client
    ):
        mock_pool.urlopen.return_value = _ok_response(
            json.dumps(
                {
                    "CreateAccessKeyResult": {
                        "AccessKey": {
                            "AccessKeyId": "X",
                            "SecretAccessKey": "Y",
                            "UserName": "n",
                        }
                    }
                }
            ).encode()
        )

        create_access_key_with_buckets_role(mock_s3_client, "n", [])
        assert mock_pool.urlopen.call_args.args[1].startswith(
            "https://iam.test"
        )


class TestDeleteAccessKey:
    @patch("tigris_boto3_ext._iam._iam_pool")
    @patch("tigris_boto3_ext._iam.SigV4Auth")
    def test_sends_delete_form(self, _sigv4, mock_pool, mock_s3_client):
        mock_pool.urlopen.return_value = _ok_response(b"")
        delete_access_key(mock_s3_client, "AKIAEXAMPLE")

        args, kwargs = mock_pool.urlopen.call_args
        assert args[0] == "POST"
        assert "/?Action=DeleteAccessKey" in args[1]
        sent = kwargs["body"]
        if isinstance(sent, bytes):
            sent = sent.decode()
        params = parse_qs(sent)
        assert params["Action"] == ["DeleteAccessKey"]
        assert params["Version"] == ["2010-05-08"]
        assert params["AccessKeyId"] == ["AKIAEXAMPLE"]

    @patch("tigris_boto3_ext._iam._iam_pool")
    @patch("tigris_boto3_ext._iam.SigV4Auth")
    def test_404_raises(self, _sigv4, mock_pool, mock_s3_client):
        response = MagicMock()
        response.status = 404
        response.data = b"not found"
        mock_pool.urlopen.return_value = response
        with pytest.raises(TigrisIAMError):
            delete_access_key(mock_s3_client, "AKIA")
