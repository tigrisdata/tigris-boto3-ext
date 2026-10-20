"""Unit tests for account storage stats."""

import json
from unittest.mock import patch

from tigris_boto3_ext import get_stats
from tigris_boto3_ext._transport import TigrisResponse


def _page(payload):
    return TigrisResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode(),
    )


class TestGetStats:
    @patch("tigris_boto3_ext.buckets.listing.signed_request")
    def test_sums_the_per_page_stats_across_all_pages(
        self, mock_request, mock_s3_client
    ):
        mock_request.side_effect = [
            _page(
                {
                    "Buckets": {"Bucket": [{"Name": "a"}, {"Name": "b"}]},
                    "Stats": {
                        "ActiveBuckets": 2,
                        "TotalStorageBytes": 100,
                        "TotalObjects": 10,
                        "TotalUniqueObjects": 8,
                    },
                    "ContinuationToken": "b",
                }
            ),
            _page(
                {
                    "Buckets": {"Bucket": [{"Name": "c"}]},
                    "Stats": {
                        "ActiveBuckets": 1,
                        "TotalStorageBytes": 5,
                        "TotalObjects": 1,
                        "TotalUniqueObjects": 1,
                    },
                }
            ),
        ]

        stats = get_stats(mock_s3_client)

        assert stats == {
            "active_buckets": 3,
            "total_objects": 11,
            "total_storage_bytes": 105,
            "total_unique_objects": 9,
        }
        first, second = (c[1]["query"] for c in mock_request.call_args_list)
        assert first == {"IncludeStats": "true", "max-buckets": "50"}
        assert second == {
            "IncludeStats": "true",
            "max-buckets": "50",
            "continuation-token": "b",
        }

    @patch("tigris_boto3_ext.buckets.listing.signed_request")
    def test_empty_account(self, mock_request, mock_s3_client):
        mock_request.return_value = _page({"Buckets": {}})

        assert get_stats(mock_s3_client) == {
            "active_buckets": 0,
            "total_objects": 0,
            "total_storage_bytes": 0,
            "total_unique_objects": 0,
        }
