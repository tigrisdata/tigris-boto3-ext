"""Unit tests for bucket info."""

import json
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from tigris_boto3_ext import (
    TigrisAPIError,
    get_bucket_info,
    get_bucket_metadata,
    has_snapshot_enabled,
)
from tigris_boto3_ext._transport import TigrisResponse
from tigris_boto3_ext.buckets._payload import parse_timestamp


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


CHILD_METADATA = {
    "ForkInfo": {
        "HasChildren": False,
        "Parents": [
            {
                "BucketName": "production-data",
                "Snapshot": "1791986199387845290; name=v1",
                "SnapshotCreatedAt": "2026-10-14T13:56:39.392723771Z",
                "ForkCreatedAt": "2026-10-14T13:56:39.395637075Z",
            }
        ],
    },
    "cache_control": "",
    "created_at": "2026-10-14T13:56:39.395637075Z",
    "estimated_rows": 0,
    "estimated_size": 0,
    "estimated_unique_rows": 0,
    "name": "dev-environment",
    "object_regions": "",
    "storage_class": "STANDARD",
    "type": 1,
}

PARENT_WITH_SIZE_METADATA = {
    "ForkInfo": {"HasChildren": True, "Parents": []},
    "created_at": "2026-10-14T13:56:38.322992894Z",
    "estimated_rows": 3,
    "estimated_size": 3072,
    "estimated_unique_rows": 3,
    "name": "production-data",
    "object_regions": "iad,sjc",
    "storage_class": "STANDARD_IA",
    "type": 1,
}

SOFT_DELETE_METADATA = {
    "cache_control": "max-age=60",
    "created_at": "2026-10-14T13:56:39.809719465Z",
    "name": "archive",
    "object_regions": "",
    "storage_class": "STANDARD",
    "soft_delete": {"enabled": True, "retention_days": 30},
    "acl_settings": {"allow_object_acl": True},
    "website": {"domain_name": "files.example.com"},
}


def _rest_response(payload):
    return TigrisResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode(),
    )


class TestGetBucketMetadata:
    @patch("tigris_boto3_ext.buckets.info.signed_request")
    def test_sends_the_metadata_request(self, mock_request, mock_s3_client):
        mock_request.return_value = _rest_response(CHILD_METADATA)

        get_bucket_metadata(mock_s3_client, "dev-environment")

        mock_request.assert_called_once_with(
            mock_s3_client,
            "GET",
            "dev-environment",
            query={"metadata": None},
            headers={"Accept": "application/json"},
        )

    @patch("tigris_boto3_ext.buckets.info.signed_request")
    def test_with_size_adds_the_flag(self, mock_request, mock_s3_client):
        mock_request.return_value = _rest_response(PARENT_WITH_SIZE_METADATA)

        get_bucket_metadata(mock_s3_client, "production-data", with_size=True)

        assert mock_request.call_args[1]["query"] == {
            "metadata": None,
            "with-size": "true",
        }

    @patch("tigris_boto3_ext.buckets.info.signed_request")
    def test_maps_a_forked_bucket(self, mock_request, mock_s3_client):
        mock_request.return_value = _rest_response(CHILD_METADATA)

        info = get_bucket_metadata(mock_s3_client, "dev-environment")

        assert info["name"] == "dev-environment"
        assert info["created_at"] == datetime(
            2026, 10, 14, 13, 56, 39, 395637, tzinfo=timezone.utc
        )
        assert info["snapshot_enabled"] is True
        assert info["storage_class"] == "STANDARD"
        assert info["regions"] == []
        assert info["is_fork_parent"] is False
        assert info["fork_parents"] == [
            {
                "bucket": "production-data",
                "snapshot": "1791986199387845290",
                "snapshot_name": "v1",
                "forked_at": datetime(
                    2026, 10, 14, 13, 56, 39, 395637, tzinfo=timezone.utc
                ),
                "snapshot_created_at": datetime(
                    2026, 10, 14, 13, 56, 39, 392723, tzinfo=timezone.utc
                ),
            }
        ]
        assert info["soft_delete"] == {"enabled": False, "retention_days": None}
        assert info["allow_object_acl"] is False
        assert info["cache_control"] is None
        assert info["custom_domain"] is None
        assert info["size"] is None
        assert info["raw"] == CHILD_METADATA

    @patch("tigris_boto3_ext.buckets.info.signed_request")
    def test_unnamed_snapshot_lineage_has_no_name(self, mock_request, mock_s3_client):
        payload = json.loads(json.dumps(CHILD_METADATA))
        payload["ForkInfo"]["Parents"][0]["Snapshot"] = "1791986199387845290"
        mock_request.return_value = _rest_response(payload)

        (lineage,) = get_bucket_metadata(mock_s3_client, "dev-environment")[
            "fork_parents"
        ]

        assert lineage["snapshot"] == "1791986199387845290"
        assert lineage["snapshot_name"] is None

    @patch("tigris_boto3_ext.buckets.info.signed_request")
    def test_maps_a_fork_parent_with_size(self, mock_request, mock_s3_client):
        mock_request.return_value = _rest_response(PARENT_WITH_SIZE_METADATA)

        info = get_bucket_metadata(mock_s3_client, "production-data", with_size=True)

        assert info["is_fork_parent"] is True
        assert info["fork_parents"] == []
        assert info["regions"] == ["iad", "sjc"]
        assert info["storage_class"] == "STANDARD_IA"
        assert info["size"] == {"objects": 3, "bytes": 3072, "objects_all_versions": 3}

    @patch("tigris_boto3_ext.buckets.info.signed_request")
    def test_maps_a_regular_soft_delete_bucket(self, mock_request, mock_s3_client):
        mock_request.return_value = _rest_response(SOFT_DELETE_METADATA)

        info = get_bucket_metadata(mock_s3_client, "archive")

        assert info["snapshot_enabled"] is False
        assert info["is_fork_parent"] is False
        assert info["fork_parents"] == []
        assert info["soft_delete"] == {"enabled": True, "retention_days": 30}
        assert info["allow_object_acl"] is True
        assert info["cache_control"] == "max-age=60"
        assert info["custom_domain"] == "files.example.com"

    @patch("tigris_boto3_ext.buckets.info.signed_request")
    def test_gateway_errors_propagate(self, mock_request, mock_s3_client):
        mock_request.side_effect = TigrisAPIError(
            "Tigris request failed (HTTP 404): NoSuchBucket: gone",
            status_code=404,
            body="{}",
            code="NoSuchBucket",
        )

        with pytest.raises(TigrisAPIError) as exc_info:
            get_bucket_metadata(mock_s3_client, "missing")

        assert exc_info.value.code == "NoSuchBucket"

    def test_rejects_empty_bucket_name(self, mock_s3_client):
        with pytest.raises(ValueError, match="bucket_name is required"):
            get_bucket_metadata(mock_s3_client, "")


class TestParseTimestamp:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (
                "2026-10-14T13:56:39.395637075Z",
                datetime(2026, 10, 14, 13, 56, 39, 395637, tzinfo=timezone.utc),
            ),
            (
                "2026-10-14T13:56:39Z",
                datetime(2026, 10, 14, 13, 56, 39, tzinfo=timezone.utc),
            ),
            (
                "2026-10-14T13:56:39.5+00:00",
                datetime(2026, 10, 14, 13, 56, 39, 500000, tzinfo=timezone.utc),
            ),
            (
                "2026-10-14T13:56:39",
                datetime(2026, 10, 14, 13, 56, 39, tzinfo=timezone.utc),
            ),
            ("", None),
            (None, None),
            (12345, None),
            ("not a date", None),
        ],
    )
    def test_gateway_timestamp_formats(self, value, expected):
        assert parse_timestamp(value) == expected
