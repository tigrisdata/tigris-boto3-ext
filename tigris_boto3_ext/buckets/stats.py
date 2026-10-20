"""Account storage statistics."""

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext.buckets.listing import list_buckets

STATS_FIELDS = (
    "active_buckets",
    "total_objects",
    "total_storage_bytes",
    "total_unique_objects",
)

_PAGE_SIZE = 50


def get_stats(s3_client: S3Client) -> dict[str, Any]:
    """
    Account-wide storage statistics.

    Tigris reports ``Stats`` for the buckets a listing request returns, and
    computes them per request, so this walks the account 50 buckets at a time
    and sums: one request per 50 buckets, each bounded by the client's read
    timeout. Not an S3 operation; the result is a plain dict.

    Args:
        s3_client: boto3 S3 client instance

    Returns:
        {'active_buckets': int, 'total_objects': int, 'total_storage_bytes': int,
         'total_unique_objects': int}

    Raises:
        TigrisAPIError: If the gateway answers with an error

    Usage:
        stats = get_stats(s3_client)
        print(f"{stats['active_buckets']} buckets, {stats['total_storage_bytes']} bytes")
    """
    totals = dict.fromkeys(STATS_FIELDS, 0)
    token: Optional[str] = None
    while True:
        page = list_buckets(
            s3_client,
            include_stats=True,
            max_buckets=_PAGE_SIZE,
            continuation_token=token,
        )
        for key in STATS_FIELDS:
            totals[key] += (page["stats"] or {}).get(key) or 0
        token = page["continuation_token"]
        if not token:
            return totals
