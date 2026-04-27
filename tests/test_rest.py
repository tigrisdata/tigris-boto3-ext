"""Unit tests for the Tigris REST PATCH transport."""

import json
from unittest.mock import MagicMock, patch

import pytest

from tigris_boto3_ext._rest import TigrisRestError, patch_bucket_settings


@pytest.fixture
def mock_s3_client():
    client = MagicMock()
    client.meta.endpoint_url = "https://t3.storage.dev"
    client.meta.region_name = "auto"
    frozen_creds = MagicMock()
    client._request_signer._credentials.get_frozen_credentials.return_value = (
        frozen_creds
    )
    return client


class TestPatchBucketSettingsValidation:
    def test_empty_bucket_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="bucket is required"):
            patch_bucket_settings(mock_s3_client, "", {"x": 1})


class TestPatchBucketSettingsRequest:
    @patch("tigris_boto3_ext._rest._rest_pool")
    @patch("tigris_boto3_ext._rest.SigV4Auth")
    def test_sends_patch_with_json_body(self, mock_sigv4, mock_pool, mock_s3_client):
        response = MagicMock()
        response.status = 200
        response.data = b'{"bucket": "b", "updated": true}'
        mock_pool.urlopen.return_value = response

        result = patch_bucket_settings(
            mock_s3_client, "my-bucket", {"object_notifications": {}}
        )

        assert result == {"bucket": "b", "updated": True}
        assert mock_pool.urlopen.call_count == 1
        args, kwargs = mock_pool.urlopen.call_args
        assert args[0] == "PATCH"
        assert "my-bucket" in args[1]
        sent_body = kwargs["body"]
        # urllib3 may receive the body as bytes or str depending on signing.
        if isinstance(sent_body, bytes):
            sent_body = sent_body.decode()
        assert json.loads(sent_body) == {"object_notifications": {}}
        assert kwargs["headers"].get("Content-Type") == "application/json"

    @patch("tigris_boto3_ext._rest._rest_pool")
    @patch("tigris_boto3_ext._rest.SigV4Auth")
    def test_400_raises_tigris_rest_error(
        self, mock_sigv4, mock_pool, mock_s3_client
    ):
        response = MagicMock()
        response.status = 400
        response.data = b'{"error":"bad request"}'
        mock_pool.urlopen.return_value = response

        with pytest.raises(TigrisRestError) as exc_info:
            patch_bucket_settings(mock_s3_client, "b", {"x": 1})

        assert exc_info.value.status_code == 400
        assert "bad request" in exc_info.value.body

    @patch("tigris_boto3_ext._rest._rest_pool")
    @patch("tigris_boto3_ext._rest.SigV4Auth")
    def test_empty_response_returns_empty_dict(
        self, mock_sigv4, mock_pool, mock_s3_client
    ):
        response = MagicMock()
        response.status = 200
        response.data = b""
        mock_pool.urlopen.return_value = response

        result = patch_bucket_settings(mock_s3_client, "b", {"x": 1})
        assert result == {}

    @patch("tigris_boto3_ext._rest._rest_pool")
    @patch("tigris_boto3_ext._rest.SigV4Auth")
    def test_non_dict_response_returns_empty_dict(
        self, mock_sigv4, mock_pool, mock_s3_client
    ):
        response = MagicMock()
        response.status = 200
        response.data = b'["array", "instead"]'
        mock_pool.urlopen.return_value = response

        result = patch_bucket_settings(mock_s3_client, "b", {"x": 1})
        assert result == {}
