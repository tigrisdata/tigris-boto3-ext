"""Unit tests for updating bucket settings."""

import json
from unittest.mock import patch

import pytest

from tigris_boto3_ext import TigrisAPIError, update_bucket
from tigris_boto3_ext._transport import TigrisResponse

PATCH_OK = {"Bucket": "my-bucket", "Update": "success"}
JSON_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}


def _ok():
    return TigrisResponse(
        status_code=200,
        headers={"x-amz-request-id": "req-1"},
        body=json.dumps(PATCH_OK).encode(),
    )


class TestUpdateBucket:
    @patch("tigris_boto3_ext.buckets.settings.signed_request")
    def test_body_settings(self, mock_request, mock_s3_client):
        mock_request.return_value = _ok()

        result = update_bucket(
            mock_s3_client,
            "my-bucket",
            allow_object_acl=True,
            default_storage_class="STANDARD_IA",
            regions=["iad", "sjc"],
            cache_control="max-age=60",
            custom_domain="files.example.com",
            nosniff=True,
        )

        mock_request.assert_called_once()
        args, kwargs = mock_request.call_args
        assert args == (mock_s3_client, "PATCH", "my-bucket")
        assert kwargs["headers"] == JSON_HEADERS
        assert json.loads(kwargs["body"]) == {
            "acl_settings": {"allow_object_acl": True},
            "storage_class": "STANDARD_IA",
            "object_regions": "iad,sjc",
            "cache_control": "max-age=60",
            "website": {"domain_name": "files.example.com"},
            "additional_http_headers": {"X-Content-Type-Options": "nosniff"},
        }
        assert result == {"name": "my-bucket", "request_id": "req-1", "raw": PATCH_OK}

    @pytest.mark.parametrize(
        ("kwargs", "expected_headers"),
        [
            ({"access": "public"}, {"X-Amz-Acl": "public-read"}),
            ({"access": "private"}, {"X-Amz-Acl": "private"}),
            (
                {"public_list_objects": True},
                {"X-Amz-Acl-Public-List-Objects-Enabled": "true"},
            ),
            (
                {"public_list_objects": False},
                {"X-Amz-Acl-Public-List-Objects-Enabled": "false"},
            ),
        ],
    )
    @patch("tigris_boto3_ext.buckets.settings.signed_request")
    def test_access_settings_are_headers(
        self, mock_request, mock_s3_client, kwargs, expected_headers
    ):
        mock_request.return_value = _ok()

        update_bucket(mock_s3_client, "my-bucket", **kwargs)

        assert mock_request.call_args[1]["headers"] == {
            **JSON_HEADERS,
            **expected_headers,
        }
        assert mock_request.call_args[1]["body"] == "{}"

    @pytest.mark.parametrize(
        ("kwargs", "expected_body"),
        [
            ({"regions": []}, {"object_regions": ""}),
            ({"regions": ""}, {"object_regions": ""}),
            ({"regions": "usa"}, {"object_regions": "usa"}),
            ({"cache_control": ""}, {"cache_control": ""}),
            ({"nosniff": False}, {"additional_http_headers": None}),
        ],
    )
    @patch("tigris_boto3_ext.buckets.settings.signed_request")
    def test_clearing_values(self, mock_request, mock_s3_client, kwargs, expected_body):
        mock_request.return_value = _ok()

        update_bucket(mock_s3_client, "my-bucket", **kwargs)

        assert json.loads(mock_request.call_args[1]["body"]) == expected_body

    @patch("tigris_boto3_ext.buckets.settings.signed_request")
    def test_empty_response_body(self, mock_request, mock_s3_client):
        mock_request.return_value = TigrisResponse(status_code=200)

        assert (
            update_bucket(mock_s3_client, "my-bucket", cache_control="x")["raw"] == {}
        )

    @pytest.mark.parametrize(
        ("kwargs", "error", "message"),
        [
            ({}, ValueError, "nothing to update"),
            ({"access": "public-read"}, ValueError, "access must be"),
            (
                {"default_storage_class": "COLD"},
                ValueError,
                "default_storage_class must be one of",
            ),
            (
                {"public_list_objects": "true"},
                TypeError,
                "public_list_objects must be a bool",
            ),
            ({"allow_object_acl": 1}, TypeError, "allow_object_acl must be a bool"),
            ({"nosniff": "yes"}, TypeError, "nosniff must be a bool"),
        ],
    )
    @patch("tigris_boto3_ext.buckets.settings.signed_request")
    def test_rejects_bad_arguments_before_sending(
        self, mock_request, mock_s3_client, kwargs, error, message
    ):
        with pytest.raises(error, match=message):
            update_bucket(mock_s3_client, "my-bucket", **kwargs)

        mock_request.assert_not_called()

    def test_rejects_empty_bucket_name(self, mock_s3_client):
        with pytest.raises(ValueError, match="bucket_name is required"):
            update_bucket(mock_s3_client, "", cache_control="x")

    @patch("tigris_boto3_ext.buckets.settings.signed_request")
    def test_gateway_errors_propagate(self, mock_request, mock_s3_client):
        mock_request.side_effect = TigrisAPIError(
            "Tigris request failed (HTTP 400): InvalidArgument: Failed to verify CNAME",
            status_code=400,
            body="{}",
            code="InvalidArgument",
        )

        with pytest.raises(TigrisAPIError) as exc_info:
            update_bucket(
                mock_s3_client, "my-bucket", custom_domain="files.example.com"
            )

        assert exc_info.value.code == "InvalidArgument"
