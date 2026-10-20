"""Integration tests for account storage stats."""

from tigris_boto3_ext import get_stats, list_buckets

from .conftest import generate_bucket_name


class TestGetStats:
    def test_account_stats(self, s3_client, test_bucket_prefix, cleanup_buckets):
        """Stats are summed over every bucket in the account."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "stats-")
        cleanup_buckets.append(bucket_name)
        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="one.txt", Body=b"x" * 10)

        stats = get_stats(s3_client)
        one_page = list_buckets(s3_client, include_stats=True, max_buckets=1)["stats"]

        assert set(stats) == {
            "active_buckets",
            "total_objects",
            "total_storage_bytes",
            "total_unique_objects",
        }
        assert all(isinstance(v, int) for v in stats.values())
        assert stats["active_buckets"] >= 1
        # A single-bucket page can never exceed the account totals.
        assert one_page["active_buckets"] == 1
        assert one_page["total_storage_bytes"] <= stats["total_storage_bytes"]
