"""Unit tests for the payload parsers shared by the non-S3 endpoints."""

from datetime import datetime, timezone

from tigris_boto3_ext.buckets._payload import (
    listing_entries,
    parse_bucket_entry,
    parse_fork_info,
    parse_listing_page,
    parse_notifications,
    parse_soft_delete,
)

NEXT_PAGE = "softdel"  # an opaque listing cursor, not a secret

# Entries as the gateway sent them in the listing probe (2026-10-20).
PLAIN_ENTRY = {
    "Name": "softdel",
    "CreationDate": "2026-10-20T10:57:30Z",
    "InitialCreatedDate": "2026-10-20T10:57:29Z",
    "SoftDeleteInfo": {"Enabled": True, "RetentionDays": 14},
}
FULL_ENTRY = {
    "Name": "child-snap",
    "CreationDate": "2026-10-20T10:57:28Z",
    "InitialCreatedDate": "2026-10-20T10:57:28Z",
    "Visibility": {"IsPublic": False},
    "OwnerInfo": {"OwnerUserId": "tid_x", "OwnerHumanUserId": "someone@example.com"},
    "Type": "Snapshot",
    "ForkInfo": {
        "HasChildren": False,
        "Parents": [
            {
                "BucketName": "parent",
                "Snapshot": "1792493847118382304; name=v1",
                "SnapshotCreatedAt": "2026-10-20T10:57:27.118460609Z",
                "ForkCreatedAt": "2026-10-20T10:57:28.204938449Z",
            }
        ],
    },
}
REGULAR_ENTRY = {
    "Name": "regional",
    "CreationDate": "2026-10-20T10:57:29Z",
    "Visibility": {"IsPublic": True},
    "Regions": "iad,sjc",
    "Type": "Regular",
    "ForkInfo": {"HasChildren": False, "Parents": None},
}


class TestParseForkInfo:
    def test_named_and_unnamed_snapshots(self):
        info = parse_fork_info(
            {
                "HasChildren": True,
                "Parents": [
                    {"BucketName": "a", "Snapshot": "1; name=v1"},
                    {"BucketName": "b", "Snapshot": "2"},
                ],
            }
        )

        assert info["is_fork_parent"] is True
        assert [
            (p["bucket"], p["snapshot"], p["snapshot_name"])
            for p in info["fork_parents"]
        ] == [
            ("a", "1", "v1"),
            ("b", "2", None),
        ]

    def test_missing_and_null_parents(self):
        assert parse_fork_info(None) == {"is_fork_parent": False, "fork_parents": []}
        assert parse_fork_info({"HasChildren": False, "Parents": None}) == {
            "is_fork_parent": False,
            "fork_parents": [],
        }


class TestParseSoftDelete:
    def test_listing_and_metadata_spellings(self):
        assert parse_soft_delete({"Enabled": True, "RetentionDays": 14}) == {
            "enabled": True,
            "retention_days": 14,
        }
        assert parse_soft_delete({"enabled": True, "retention_days": 30}) == {
            "enabled": True,
            "retention_days": 30,
        }
        assert parse_soft_delete(None) == {"enabled": False, "retention_days": None}
        assert parse_soft_delete({}) == {"enabled": False, "retention_days": None}


class TestParseNotifications:
    def test_token_basic_and_absent(self):
        url = "https://api.example.com/hooks/tigris"
        assert parse_notifications(
            {"enabled": True, "web_hook": url, "region": "x", "auth": {"token": "****"}}
        ) == {"enabled": True, "url": url, "filter": None, "auth": "token"}
        assert parse_notifications(
            {
                "enabled": False,
                "web_hook": url,
                "filter": 'WHERE `key` REGEXP "^images/"',
                "auth": {"basic_user": "hook", "basic_pass": "****"},
            }
        ) == {
            "enabled": False,
            "url": url,
            "filter": 'WHERE `key` REGEXP "^images/"',
            "auth": "basic",
        }
        cleared = {"enabled": False, "url": None, "filter": None, "auth": None}
        assert parse_notifications({"enabled": False}) == cleared
        assert parse_notifications({}) == cleared
        assert parse_notifications(None) == cleared


class TestParseBucketEntry:
    def test_plain_entry_leaves_unrequested_fields_none(self):
        entry = parse_bucket_entry(PLAIN_ENTRY)

        assert entry == {
            "name": "softdel",
            "created_at": datetime(2026, 10, 20, 10, 57, 29, tzinfo=timezone.utc),
            "modified_at": datetime(2026, 10, 20, 10, 57, 30, tzinfo=timezone.utc),
            "snapshot_enabled": None,
            "visibility": None,
            "regions": None,
            "is_fork_parent": None,
            "fork_parents": None,
            "soft_delete": {"enabled": True, "retention_days": 14},
            "owner": None,
        }

    def test_full_entry(self):
        entry = parse_bucket_entry(FULL_ENTRY)

        assert entry["snapshot_enabled"] is True
        assert entry["visibility"] == "private"
        assert entry["regions"] is None
        assert entry["is_fork_parent"] is False
        (parent,) = entry["fork_parents"]
        assert parent["bucket"] == "parent"
        assert parent["snapshot"] == "1792493847118382304"
        assert parent["snapshot_name"] == "v1"
        assert parent["forked_at"] == datetime(
            2026, 10, 20, 10, 57, 28, 204938, tzinfo=timezone.utc
        )
        assert entry["soft_delete"] == {"enabled": False, "retention_days": None}
        assert entry["owner"] == {
            "user_id": "tid_x",
            "human_user_id": "someone@example.com",
        }

    def test_regular_public_regional_entry(self):
        entry = parse_bucket_entry(REGULAR_ENTRY)

        assert entry["snapshot_enabled"] is False
        assert entry["visibility"] == "public"
        assert entry["regions"] == ["iad", "sjc"]
        assert entry["is_fork_parent"] is False
        assert entry["fork_parents"] == []
        assert entry["created_at"] == entry["modified_at"]  # no InitialCreatedDate


class TestListingShapes:
    def test_entries_unwrap_single_and_missing(self):
        assert listing_entries({}) == []
        assert listing_entries({"Buckets": None}) == []
        assert listing_entries({"Buckets": {"Bucket": None}}) == []
        assert listing_entries({"Buckets": {"Bucket": {"Name": "one"}}}) == [
            {"Name": "one"}
        ]

    def test_listing_page(self):
        page = parse_listing_page(
            {
                "Owner": {"ID": "tid_x", "DisplayName": "me"},
                "Buckets": {"Bucket": [PLAIN_ENTRY]},
                "ContinuationToken": NEXT_PAGE,
            }
        )

        assert [b["name"] for b in page["buckets"]] == ["softdel"]
        assert page["continuation_token"] == NEXT_PAGE
        assert page["owner"] == {"id": "tid_x", "display_name": "me"}
        assert page["raw"]["ContinuationToken"] == NEXT_PAGE

    def test_listing_page_defaults(self):
        page = parse_listing_page({})

        assert page == {
            "buckets": [],
            "continuation_token": None,
            "owner": {"id": None, "display_name": None},
            "raw": {},
        }
