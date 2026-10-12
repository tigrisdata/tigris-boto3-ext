"""Unit tests for bucket info."""

import pytest

from tigris_boto3_ext import get_bucket_info, has_snapshot_enabled


def _head_bucket_response(headers):
    return {"ResponseMetadata": {"HTTPHeaders": headers}}


class TestHasSnapshotEnabled:
    @pytest.mark.parametrize(
        ("headers", "expected"),
        [
            ({"x-tigris-enable-snapshot": "true"}, True),
            ({"x-tigris-enable-snapshot": "True"}, True),
            ({"x-tigris-enable-snapshot": "false"}, False),
            ({}, False),
        ],
    )
    def test_reads_the_snapshot_header(self, mock_s3_client, headers, expected):
        mock_s3_client.head_bucket.return_value = _head_bucket_response(headers)

        assert has_snapshot_enabled(mock_s3_client, "my-bucket") is expected
        mock_s3_client.head_bucket.assert_called_once_with(Bucket="my-bucket")


class TestGetBucketInfo:
    def test_regular_bucket(self, mock_s3_client):
        response = _head_bucket_response({})
        mock_s3_client.head_bucket.return_value = response

        info = get_bucket_info(mock_s3_client, "my-bucket")

        assert info == {
            "snapshot_enabled": False,
            "fork_source_bucket": None,
            "fork_source_snapshot": None,
            "response_metadata": response,
        }
        mock_s3_client.head_bucket.assert_called_once_with(Bucket="my-bucket")

    def test_forked_bucket(self, mock_s3_client):
        mock_s3_client.head_bucket.return_value = _head_bucket_response(
            {
                "x-tigris-enable-snapshot": "true",
                "x-tigris-fork-source-bucket": "production-data",
                "x-tigris-fork-source-bucket-snapshot": "1787441627070249004",
            }
        )

        info = get_bucket_info(mock_s3_client, "dev-environment")

        assert info["snapshot_enabled"] is True
        assert info["fork_source_bucket"] == "production-data"
        assert info["fork_source_snapshot"] == "1787441627070249004"
