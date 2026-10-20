"""Unit tests for listing the forks of a bucket."""

import json
from unittest.mock import patch

import pytest

from tigris_boto3_ext import TigrisAPIError, list_forks
from tigris_boto3_ext._transport import TigrisResponse

# A fork listing as the gateway sent it in the listing probe (2026-10-20).
FORK_LISTING = {
    "Owner": {"ID": "", "DisplayName": ""},
    "Buckets": {
        "Bucket": [
            {
                "Name": "child-now",
                "CreationDate": "2026-10-20T10:57:26.329489182Z",
                "ForkInfo": {
                    "HasChildren": False,
                    "Parents": [
                        {
                            "BucketName": "parent",
                            "Snapshot": "1792493846320284327",
                            "SnapshotCreatedAt": "2026-10-20T10:57:26.325583353Z",
                            "ForkCreatedAt": "2026-10-20T10:57:26.329489182Z",
                        }
                    ],
                },
            }
        ]
    },
}


NEXT_PAGE = "after"  # an opaque listing cursor, not a secret


def _page(payload):
    return TigrisResponse(
        status_code=200,
        headers={"content-type": "application/json"},
        body=json.dumps(payload).encode(),
    )


class TestListForks:
    @patch("tigris_boto3_ext.buckets.forks.signed_request")
    def test_lists_children_with_lineage(self, mock_request, mock_s3_client):
        mock_request.return_value = _page(FORK_LISTING)

        page = list_forks(mock_s3_client, "parent", max_buckets=20)

        mock_request.assert_called_once_with(
            mock_s3_client,
            "GET",
            "",
            query={"max-buckets": "20"},
            headers={"Accept": "application/json", "X-Tigris-Fork": "parent"},
        )
        (child,) = page["buckets"]
        assert child["name"] == "child-now"
        assert child["is_fork_parent"] is False
        (lineage,) = child["fork_parents"]
        assert lineage["bucket"] == "parent"
        assert lineage["snapshot"] == "1792493846320284327"
        assert lineage["snapshot_name"] is None
        assert child["created_at"] == lineage["forked_at"]
        assert page["continuation_token"] is None
        assert "owner" not in page
        assert page["raw"] == FORK_LISTING

    @patch("tigris_boto3_ext.buckets.forks.signed_request")
    def test_bucket_without_forks(self, mock_request, mock_s3_client):
        mock_request.return_value = _page({"Owner": {"ID": "", "DisplayName": ""}})

        assert list_forks(mock_s3_client, "lonely")["buckets"] == []

    @patch("tigris_boto3_ext.buckets.forks.signed_request")
    def test_continuation_token_is_forwarded(self, mock_request, mock_s3_client):
        mock_request.return_value = _page({})

        list_forks(mock_s3_client, "parent", continuation_token=NEXT_PAGE)

        assert mock_request.call_args[1]["query"] == {"continuation-token": NEXT_PAGE}

    def test_rejects_empty_source(self, mock_s3_client):
        with pytest.raises(ValueError, match="source_bucket is required"):
            list_forks(mock_s3_client, "")

    @patch("tigris_boto3_ext.buckets.forks.signed_request")
    def test_access_denied_propagates(self, mock_request, mock_s3_client):
        mock_request.side_effect = TigrisAPIError(
            "Tigris request failed (HTTP 403): AccessDenied: Access Denied.",
            status_code=403,
            body="",
            code="AccessDenied",
        )

        with pytest.raises(TigrisAPIError) as exc_info:
            list_forks(mock_s3_client, "not-mine")

        assert exc_info.value.status_code == 403
