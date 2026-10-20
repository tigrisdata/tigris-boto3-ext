"""Integration tests for bucket listing and fork listing."""

from datetime import datetime

import pytest

from tigris_boto3_ext import (
    TigrisAPIError,
    create_bucket,
    create_fork,
    create_snapshot,
    create_snapshot_bucket,
    get_snapshot_version,
    list_buckets,
    list_forks,
)

from .conftest import generate_bucket_name


def _ours(page, prefix):
    return {b["name"]: b for b in page["buckets"] if b["name"].startswith(prefix)}


class TestListBuckets:
    def test_flags_fill_the_tigris_fields(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Type, visibility, regions and fork info come back normalized when asked for."""
        prefix = generate_bucket_name(test_bucket_prefix, "listing-")
        parent, child = f"{prefix}-parent", f"{prefix}-child"
        public, regional = f"{prefix}-public", f"{prefix}-regional"
        cleanup_buckets.extend([child, parent, public, regional])
        create_snapshot_bucket(s3_client, parent)
        version = get_snapshot_version(
            create_snapshot(s3_client, parent, snapshot_name="v1")
        )
        create_fork(s3_client, child, parent, snapshot_version=version)
        create_bucket(s3_client, public, access="public")
        create_bucket(s3_client, regional, regions=["iad", "sjc"])

        # Names sort together, so a cursor just before the prefix reaches them in one page.
        page = list_buckets(
            s3_client,
            include_regions=True,
            include_type=True,
            include_visibility=True,
            include_fork_info=True,
            include_owner=True,
            max_buckets=50,
            continuation_token=prefix[:-1],
        )
        ours = _ours(page, prefix)

        assert set(ours) == {parent, child, public, regional}
        assert page["owner"]["id"]
        for bucket in ours.values():
            assert isinstance(bucket["created_at"], datetime)
            assert bucket["owner"]["user_id"]

        assert ours[parent]["snapshot_enabled"] is True
        assert ours[child]["snapshot_enabled"] is True
        assert ours[public]["snapshot_enabled"] is False
        assert ours[public]["visibility"] == "public"
        assert ours[parent]["visibility"] == "private"
        assert ours[regional]["regions"] == ["iad", "sjc"]
        assert ours[public]["regions"] == []
        (lineage,) = ours[child]["fork_parents"]
        assert lineage["bucket"] == parent
        assert lineage["snapshot"] == version
        assert lineage["snapshot_name"] == "v1"
        assert ours[public]["fork_parents"] == []
        assert ours[public]["soft_delete"] == {"enabled": False, "retention_days": None}

    def test_plain_listing_leaves_unrequested_fields_none(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Without flags the entry carries only name, dates and soft-delete state."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "listing-plain-")
        cleanup_buckets.append(bucket_name)
        s3_client.create_bucket(Bucket=bucket_name)

        page = list_buckets(
            s3_client, max_buckets=50, continuation_token=bucket_name[:-1]
        )
        entry = _ours(page, bucket_name)[bucket_name]

        assert entry["snapshot_enabled"] is None
        assert entry["visibility"] is None
        assert entry["regions"] is None
        assert entry["fork_parents"] is None
        assert entry["owner"] is None
        assert page["stats"] is None

    def test_only_forks(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """only_forks filters the listing to forked buckets."""
        prefix = generate_bucket_name(test_bucket_prefix, "listing-only-")
        parent, child = f"{prefix}-parent", f"{prefix}-child"
        cleanup_buckets.extend([child, parent])
        create_snapshot_bucket(s3_client, parent)
        create_fork(s3_client, child, parent)

        page = list_buckets(
            s3_client,
            only_forks=True,
            include_fork_info=True,
            max_buckets=50,
            continuation_token=prefix[:-1],
        )
        ours = _ours(page, prefix)

        assert set(ours) == {child}
        assert ours[child]["fork_parents"][0]["bucket"] == parent

    def test_paging(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """A page of one is truncated once the account has more than one bucket."""
        for suffix in ("paging-first-", "paging-second-"):
            bucket_name = generate_bucket_name(test_bucket_prefix, suffix)
            cleanup_buckets.append(bucket_name)
            s3_client.create_bucket(Bucket=bucket_name)

        first = list_buckets(s3_client, max_buckets=1)

        assert len(first["buckets"]) == 1
        assert first["continuation_token"]
        second = list_buckets(
            s3_client, max_buckets=1, continuation_token=first["continuation_token"]
        )
        assert second["buckets"][0]["name"] != first["buckets"][0]["name"]


class TestListForks:
    def test_children_of_a_parent(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """The forks of a bucket are listed with their lineage; others are not."""
        prefix = generate_bucket_name(test_bucket_prefix, "forks-")
        parent, child_now, child_snap = (
            f"{prefix}-parent",
            f"{prefix}-now",
            f"{prefix}-snap",
        )
        cleanup_buckets.extend([child_now, child_snap, parent])
        create_snapshot_bucket(s3_client, parent)
        create_fork(s3_client, child_now, parent)
        version = get_snapshot_version(
            create_snapshot(s3_client, parent, snapshot_name="v1")
        )
        create_fork(s3_client, child_snap, parent, snapshot_version=version)

        page = list_forks(s3_client, parent)
        by_name = {b["name"]: b for b in page["buckets"]}

        assert set(by_name) == {child_now, child_snap}
        assert by_name[child_snap]["fork_parents"][0]["snapshot"] == version
        assert by_name[child_snap]["fork_parents"][0]["snapshot_name"] == "v1"
        assert by_name[child_now]["fork_parents"][0]["snapshot_name"] is None
        assert isinstance(by_name[child_now]["created_at"], datetime)
        assert "owner" not in page

    def test_bucket_without_forks(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """A bucket nothing was forked from has an empty fork list."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "forks-none-")
        cleanup_buckets.append(bucket_name)
        create_snapshot_bucket(s3_client, bucket_name)

        assert list_forks(s3_client, bucket_name)["buckets"] == []

    def test_unknown_source_is_access_denied(self, s3_client, test_bucket_prefix):
        """The gateway answers AccessDenied, not NoSuchBucket, for an unknown source."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "forks-missing-")

        with pytest.raises(TigrisAPIError) as exc_info:
            list_forks(s3_client, bucket_name)

        assert exc_info.value.status_code == 403
        assert exc_info.value.code == "AccessDenied"
