"""Unit tests for the object_notifications public module."""

import json
from unittest.mock import MagicMock, patch

import pytest

from tigris_boto3_ext.object_notifications import (
    ObjectNotificationsError,
    clear_object_notifications,
    set_object_notifications,
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


def _ok():
    response = MagicMock()
    response.status = 200
    response.data = b""
    return response


class TestSetObjectNotifications:
    @patch("tigris_boto3_ext.object_notifications._pool")
    @patch("tigris_boto3_ext.object_notifications.SigV4Auth")
    def test_minimal(self, mock_sigv4, mock_pool, mock_s3_client):
        mock_pool.urlopen.return_value = _ok()

        set_object_notifications(
            mock_s3_client, "b", webhook_url="https://hook.example"
        )

        args, kwargs = mock_pool.urlopen.call_args
        assert args[0] == "PATCH"
        assert "/b" in args[1]
        body = kwargs["body"]
        if isinstance(body, bytes):
            body = body.decode()
        assert json.loads(body) == {
            "object_notifications": {
                "enabled": True,
                "web_hook": "https://hook.example",
                "filter": "",
            }
        }
        # SigV4 needs the body hash in canonical headers.
        assert "X-Amz-Content-Sha256" in kwargs["headers"]

    @patch("tigris_boto3_ext.object_notifications._pool")
    @patch("tigris_boto3_ext.object_notifications.SigV4Auth")
    def test_with_filter(self, _sigv4, mock_pool, mock_s3_client):
        mock_pool.urlopen.return_value = _ok()
        set_object_notifications(
            mock_s3_client,
            "b",
            webhook_url="https://hook.example",
            event_filter='WHERE `key` REGEXP "^results/"',
        )
        body = mock_pool.urlopen.call_args.kwargs["body"]
        if isinstance(body, bytes):
            body = body.decode()
        assert (
            json.loads(body)["object_notifications"]["filter"]
            == 'WHERE `key` REGEXP "^results/"'
        )

    @patch("tigris_boto3_ext.object_notifications._pool")
    @patch("tigris_boto3_ext.object_notifications.SigV4Auth")
    def test_with_token_auth(self, _sigv4, mock_pool, mock_s3_client):
        mock_pool.urlopen.return_value = _ok()
        set_object_notifications(
            mock_s3_client, "b", webhook_url="https://h", auth_token="t"
        )
        body = mock_pool.urlopen.call_args.kwargs["body"]
        if isinstance(body, bytes):
            body = body.decode()
        assert json.loads(body)["object_notifications"]["auth"] == {"token": "t"}

    @patch("tigris_boto3_ext.object_notifications._pool")
    @patch("tigris_boto3_ext.object_notifications.SigV4Auth")
    def test_with_basic_auth(self, _sigv4, mock_pool, mock_s3_client):
        mock_pool.urlopen.return_value = _ok()
        set_object_notifications(
            mock_s3_client,
            "b",
            webhook_url="https://h",
            auth_username="u",
            auth_password="p",  # noqa: S106
        )
        body = mock_pool.urlopen.call_args.kwargs["body"]
        if isinstance(body, bytes):
            body = body.decode()
        assert json.loads(body)["object_notifications"]["auth"] == {
            "basic_user": "u",
            "basic_pass": "p",
        }

    def test_empty_url_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="webhook_url is required"):
            set_object_notifications(mock_s3_client, "b", webhook_url="")

    def test_non_http_scheme_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="must use http or https"):
            set_object_notifications(
                mock_s3_client, "b", webhook_url="ftp://hook"
            )

    def test_token_with_basic_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="cannot be combined"):
            set_object_notifications(
                mock_s3_client,
                "b",
                webhook_url="https://h",
                auth_token="t",
                auth_username="u",
                auth_password="p",  # noqa: S106
            )

    def test_partial_basic_auth_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="must be provided together"):
            set_object_notifications(
                mock_s3_client,
                "b",
                webhook_url="https://h",
                auth_username="u",
            )

    @patch("tigris_boto3_ext.object_notifications._pool")
    @patch("tigris_boto3_ext.object_notifications.SigV4Auth")
    def test_400_raises(self, _sigv4, mock_pool, mock_s3_client):
        response = MagicMock()
        response.status = 400
        response.data = b"<Error>bad</Error>"
        mock_pool.urlopen.return_value = response
        with pytest.raises(ObjectNotificationsError) as exc_info:
            set_object_notifications(
                mock_s3_client, "b", webhook_url="https://h"
            )
        assert exc_info.value.status_code == 400
        assert "bad" in exc_info.value.body

    def test_empty_bucket_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="bucket is required"):
            set_object_notifications(
                mock_s3_client, "", webhook_url="https://h"
            )


class TestClearObjectNotifications:
    @patch("tigris_boto3_ext.object_notifications._pool")
    @patch("tigris_boto3_ext.object_notifications.SigV4Auth")
    def test_clears(self, _sigv4, mock_pool, mock_s3_client):
        mock_pool.urlopen.return_value = _ok()
        clear_object_notifications(mock_s3_client, "b")
        body = mock_pool.urlopen.call_args.kwargs["body"]
        if isinstance(body, bytes):
            body = body.decode()
        assert json.loads(body) == {"object_notifications": {}}
