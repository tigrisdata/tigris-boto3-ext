"""Unit tests for bucket listing and account stats."""

import json
from unittest.mock import patch

import pytest

from tigris_boto3_ext import TigrisAPIError, list_buckets
from tigris_boto3_ext._transport import TigrisResponse

JSON = {"Accept": "application/json"}
NEXT_PAGE = "after-this"  # an opaque listing cursor, not a secret


def _page(payload):
    return TigrisResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode(),
    )


class TestListBuckets:
    @patch("tigris_boto3_ext.buckets.listing.signed_request")
    def test_no_flags_sends_a_bare_listing(self, mock_request, mock_s3_client):
        mock_request.return_value = _page(
            {
                "Owner": {"ID": "tid_x", "DisplayName": "tid_x"},
                "Buckets": {
                    "Bucket": [{"Name": "a", "CreationDate": "2026-10-20T10:57:26Z"}]
                },
            }
        )

        page = list_buckets(mock_s3_client)

        mock_request.assert_called_once_with(
            mock_s3_client, "GET", "", query={}, headers=JSON
        )
        assert [b["name"] for b in page["buckets"]] == ["a"]
        assert page["buckets"][0]["regions"] is None
        assert page["continuation_token"] is None
        assert page["owner"] == {"id": "tid_x", "display_name": "tid_x"}
        assert page["stats"] is None

    @patch("tigris_boto3_ext.buckets.listing.signed_request")
    def test_flags_and_paging_become_query_parameters(
        self, mock_request, mock_s3_client
    ):
        mock_request.return_value = _page({"Buckets": {"Bucket": []}})

        list_buckets(
            mock_s3_client,
            include_regions=True,
            include_type=True,
            include_visibility=True,
            include_fork_info=True,
            include_owner=True,
            include_stats=True,
            only_forks=True,
            owned_by="someone@example.com",
            max_buckets=20,
            continuation_token=NEXT_PAGE,
        )

        assert mock_request.call_args[1]["query"] == {
            "IncludeRegionsInfo": "true",
            "IncludeTypeInfo": "true",
            "IncludeVisibility": "true",
            "IncludeForkInfo": "true",
            "IncludeOwnerInfo": "true",
            "IncludeStats": "true",
            "OnlyForked": "true",
            "OwnedBy": "someone@example.com",
            "max-buckets": "20",
            "continuation-token": NEXT_PAGE,
        }

    @patch("tigris_boto3_ext.buckets.listing.signed_request")
    def test_requested_regions_default_to_global(self, mock_request, mock_s3_client):
        mock_request.return_value = _page(
            {
                "Buckets": {
                    "Bucket": [{"Name": "g"}, {"Name": "r", "Regions": "iad,sjc"}]
                }
            }
        )

        page = list_buckets(mock_s3_client, include_regions=True)

        assert [b["regions"] for b in page["buckets"]] == [[], ["iad", "sjc"]]

    @patch("tigris_boto3_ext.buckets.listing.signed_request")
    def test_page_stats(self, mock_request, mock_s3_client):
        mock_request.return_value = _page(
            {
                "Buckets": {"Bucket": [{"Name": "a"}]},
                "Stats": {
                    "ActiveBuckets": 1,
                    "TotalStorageBytes": 56624483278,
                    "TotalObjects": 2306,
                    "TotalUniqueObjects": 2102,
                },
            }
        )

        page = list_buckets(mock_s3_client, include_stats=True)

        assert page["stats"] == {
            "active_buckets": 1,
            "total_objects": 2306,
            "total_storage_bytes": 56624483278,
            "total_unique_objects": 2102,
        }

    @pytest.mark.parametrize("value", [0, -5, True, 2.0, "20"])
    def test_rejects_bad_page_sizes(self, mock_s3_client, value):
        with pytest.raises(ValueError, match="positive integer"):
            list_buckets(mock_s3_client, max_buckets=value)

    @patch("tigris_boto3_ext.buckets.listing.signed_request")
    def test_gateway_errors_propagate(self, mock_request, mock_s3_client):
        mock_request.side_effect = TigrisAPIError(
            "boom", status_code=403, body="", code="AccessDenied"
        )

        with pytest.raises(TigrisAPIError) as exc_info:
            list_buckets(mock_s3_client)

        assert exc_info.value.code == "AccessDenied"
