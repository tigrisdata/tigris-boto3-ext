"""Tests for bundle_objects function."""

from unittest.mock import MagicMock, patch

import pytest

from tigris_boto3_ext.bundle import (
    BUNDLE_COMPRESSION_GZIP,
    BUNDLE_COMPRESSION_NONE,
    BUNDLE_COMPRESSION_ZSTD,
    BUNDLE_ON_ERROR_FAIL,
    BUNDLE_ON_ERROR_SKIP,
    MAX_BUNDLE_KEYS,
    BundleError,
    BundleResponse,
    bundle_objects,
)


@pytest.fixture
def mock_s3_client():
    """Create a mock boto3 S3 client with endpoint and credentials."""
    client = MagicMock()
    client.meta.endpoint_url = "https://t3.storage.dev"
    client.meta.region_name = "auto"
    frozen_creds = MagicMock()
    client._request_signer._credentials.get_frozen_credentials.return_value = (
        frozen_creds
    )
    return client


class TestBundleObjectsValidation:
    def test_empty_bucket_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="bucket is required"):
            bundle_objects(mock_s3_client, "", ["key1"])

    def test_empty_keys_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="at least one key"):
            bundle_objects(mock_s3_client, "bucket", [])

    def test_none_keys_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="at least one key"):
            bundle_objects(mock_s3_client, "bucket", None)

    def test_invalid_compression_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="invalid compression"):
            bundle_objects(
                mock_s3_client, "bucket", ["key"], compression="lz4"
            )

    def test_invalid_on_error_raises(self, mock_s3_client):
        with pytest.raises(ValueError, match="invalid on_error"):
            bundle_objects(
                mock_s3_client, "bucket", ["key"], on_error="panic"
            )

    def test_too_many_keys_raises(self, mock_s3_client):
        keys = [f"key_{i}" for i in range(MAX_BUNDLE_KEYS + 1)]
        with pytest.raises(ValueError, match="too many keys"):
            bundle_objects(mock_s3_client, "bucket", keys)

    def test_max_keys_exactly_at_limit(self, mock_s3_client):
        """Exactly MAX_BUNDLE_KEYS should not raise a validation error."""
        keys = [f"key_{i}" for i in range(MAX_BUNDLE_KEYS)]
        with patch("tigris_boto3_ext.bundle._bundle_pool") as mock_pool, patch(
            "tigris_boto3_ext.bundle.SigV4Auth"
        ):
            mock_response = MagicMock()
            mock_response.status = 200
            mock_response.headers = {"Content-Type": "application/x-tar"}
            mock_pool.urlopen.return_value = mock_response
            result = bundle_objects(mock_s3_client, "bucket", keys)
            assert isinstance(result, BundleResponse)


class TestBundleObjectsRequest:
    @patch("tigris_boto3_ext.bundle._bundle_pool")
    @patch("tigris_boto3_ext.bundle.SigV4Auth")
    def test_sends_correct_request(self, mock_sigv4, mock_pool, mock_s3_client):
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "application/x-tar"}
        mock_response.content = b""

        mock_pool.urlopen.return_value = mock_response

        result = bundle_objects(
            mock_s3_client,
            "my-bucket",
            ["a.jpg", "b.jpg"],
        )

        # Verify SigV4 was called.
        mock_sigv4.assert_called_once()

        # Verify session.send was called.
        mock_pool.urlopen.assert_called_once()

        # Verify the request args (method, url).
        call_args = mock_pool.urlopen.call_args
        method = call_args[0][0]
        url = call_args[0][1]
        assert method == "POST"
        assert "my-bucket" in url
        assert "bundle" in url

        assert isinstance(result, BundleResponse)
        assert result.content_type == "application/x-tar"
        assert result.status_code == 200

    @patch("tigris_boto3_ext.bundle._bundle_pool")
    @patch("tigris_boto3_ext.bundle.SigV4Auth")
    def test_default_options(self, mock_sigv4, mock_pool, mock_s3_client):
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "application/x-tar"}
        mock_pool.urlopen.return_value = mock_response

        bundle_objects(mock_s3_client, "bucket", ["key"])

        # Check the AWSRequest that was signed.
        sigv4_instance = mock_sigv4.return_value
        request = sigv4_instance.add_auth.call_args[0][0]
        assert request.headers["X-Tigris-Bundle-Format"] == "tar"
        assert request.headers["X-Tigris-Bundle-Compression"] == "none"
        assert request.headers["X-Tigris-Bundle-On-Error"] == "skip"
        assert request.headers["Content-Type"] == "application/json"

    @patch("tigris_boto3_ext.bundle._bundle_pool")
    @patch("tigris_boto3_ext.bundle.SigV4Auth")
    def test_custom_compression_and_error_mode(
        self, mock_sigv4, mock_pool, mock_s3_client
    ):
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.headers = {"Content-Type": "application/gzip"}
        mock_pool.urlopen.return_value = mock_response

        result = bundle_objects(
            mock_s3_client,
            "bucket",
            ["key"],
            compression=BUNDLE_COMPRESSION_GZIP,
            on_error=BUNDLE_ON_ERROR_FAIL,
        )

        sigv4_instance = mock_sigv4.return_value
        request = sigv4_instance.add_auth.call_args[0][0]
        assert request.headers["X-Tigris-Bundle-Compression"] == "gzip"
        assert request.headers["X-Tigris-Bundle-On-Error"] == "fail"
        assert result.content_type == "application/gzip"


class TestBundleObjectsErrors:
    @patch("tigris_boto3_ext.bundle._bundle_pool")
    @patch("tigris_boto3_ext.bundle.SigV4Auth")
    def test_http_error_raises(self, mock_sigv4, mock_pool, mock_s3_client):
        mock_response = MagicMock()
        mock_response.status = 400
        mock_response.read.return_value = b"<Error><Code>InvalidArgument</Code></Error>"
        mock_pool.urlopen.return_value = mock_response

        with pytest.raises(BundleError, match="HTTP 400") as exc_info:
            bundle_objects(mock_s3_client, "bucket", ["key"])

        assert exc_info.value.status_code == 400
        assert "InvalidArgument" in exc_info.value.body

    @patch("tigris_boto3_ext.bundle._bundle_pool")
    @patch("tigris_boto3_ext.bundle.SigV4Auth")
    def test_http_error_with_unreadable_body(
        self, mock_sigv4, mock_pool, mock_s3_client
    ):
        """Error body read failure should still raise and close the response."""
        mock_response = MagicMock()
        mock_response.status = 500
        mock_response.read.side_effect = OSError("connection reset")
        mock_pool.urlopen.return_value = mock_response

        with pytest.raises(BundleError, match="HTTP 500") as exc_info:
            bundle_objects(mock_s3_client, "bucket", ["key"])

        assert exc_info.value.status_code == 500
        assert exc_info.value.body == ""
        mock_response.close.assert_called_once()


class TestBundleResponse:
    def test_context_manager(self):
        mock_body = MagicMock()
        resp = BundleResponse(
            body=mock_body,
            content_type="application/x-tar",
            status_code=200,
            headers={},
        )

        with resp as r:
            assert r is resp

        mock_body.close.assert_called_once()

    def test_read_delegates(self):
        mock_body = MagicMock()
        mock_body.read.return_value = b"data"
        resp = BundleResponse(
            body=mock_body,
            content_type="application/x-tar",
            status_code=200,
            headers={},
        )

        assert resp.read(4) == b"data"
        mock_body.read.assert_called_once_with(4)

    def test_read_all(self):
        mock_body = MagicMock()
        mock_body.read.return_value = b"all data"
        resp = BundleResponse(
            body=mock_body,
            content_type="application/x-tar",
            status_code=200,
            headers={},
        )

        assert resp.read() == b"all data"
        mock_body.read.assert_called_once_with(None)

    def test_object_count_property(self):
        resp = BundleResponse(
            body=MagicMock(),
            content_type="application/x-tar",
            status_code=200,
            headers={"x-tigris-bundle-count": "42"},
        )
        assert resp.object_count == 42

    def test_bundle_bytes_property(self):
        resp = BundleResponse(
            body=MagicMock(),
            content_type="application/x-tar",
            status_code=200,
            headers={"x-tigris-bundle-bytes": "614400"},
        )
        assert resp.bundle_bytes == 614400

    def test_skipped_count_property(self):
        resp = BundleResponse(
            body=MagicMock(),
            content_type="application/x-tar",
            status_code=200,
            headers={"x-tigris-bundle-skipped": "3"},
        )
        assert resp.skipped_count == 3

    def test_metadata_properties_missing_headers(self):
        resp = BundleResponse(
            body=MagicMock(),
            content_type="application/x-tar",
            status_code=200,
            headers={},
        )
        assert resp.object_count is None
        assert resp.bundle_bytes is None
        assert resp.skipped_count is None

    def test_metadata_properties_case_insensitive(self):
        """Headers in Title-Case (HTTP/1.1) should still be found."""
        resp = BundleResponse(
            body=MagicMock(),
            content_type="application/x-tar",
            status_code=200,
            headers={
                "X-Tigris-Bundle-Count": "10",
                "X-Tigris-Bundle-Bytes": "2048",
                "X-Tigris-Bundle-Skipped": "1",
            },
        )
        assert resp.object_count == 10
        assert resp.bundle_bytes == 2048
        assert resp.skipped_count == 1
