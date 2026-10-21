"""Bucket listing with Tigris details."""

from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._transport import paging_params, signed_request
from tigris_boto3_ext.buckets._payload import parse_listing_page

_STATS_FIELDS = {
    "active_buckets": "ActiveBuckets",
    "total_objects": "TotalObjects",
    "total_storage_bytes": "TotalStorageBytes",
    "total_unique_objects": "TotalUniqueObjects",
}


def list_buckets(
    s3_client: S3Client,
    *,
    include_regions: bool = False,
    include_type: bool = False,
    include_visibility: bool = False,
    include_fork_info: bool = False,
    include_owner: bool = False,
    include_stats: bool = False,
    only_forks: bool = False,
    owned_by: Optional[str] = None,
    max_buckets: Optional[int] = None,
    continuation_token: Optional[str] = None,
) -> dict[str, Any]:
    """
    List one page of buckets with details boto3's ``list_buckets`` does not carry.

    This reads Tigris's ``GET /`` listing with its ``Include*`` flags, which is
    not an S3 operation, so the result is a plain dict. Each flag adds a field
    to the entries; fields you did not ask for are None.

    Args:
        s3_client: boto3 S3 client instance
        include_regions: Fill ``regions`` (``[]`` means global placement)
        include_type: Fill ``snapshot_enabled``
        include_visibility: Fill ``visibility`` (``"public"`` or ``"private"``)
        include_fork_info: Fill ``is_fork_parent`` and ``fork_parents``
        include_owner: Fill ``owner`` (``user_id``, ``human_user_id``)
        include_stats: Add ``stats`` for the buckets on this page
        only_forks: Only buckets that are forks of another bucket
        owned_by: Only buckets owned by this user, by username (an email address)
        max_buckets: Page size; the gateway default applies when None
        continuation_token: ``continuation_token`` from the previous page

    Returns:
        {
            'buckets': [{'name', 'created_at', 'modified_at', 'snapshot_enabled',
                         'visibility', 'regions', 'is_fork_parent', 'fork_parents',
                         'soft_delete', 'owner'}],
            'continuation_token': str or None,   # None on the last page
            'owner': {'id': str, 'display_name': str},
            'stats': {'active_buckets', 'total_objects', 'total_storage_bytes',
                      'total_unique_objects'} or None,   # this page only
            'raw': dict,
        }
        ``soft_delete`` is always filled (``{'enabled': False, 'retention_days':
        None}`` when off). ``stats`` covers the buckets on the page, not the
        account; ``get_stats`` in ``buckets/stats.py`` sums the account.

    Raises:
        ValueError: If ``max_buckets`` is not a positive integer
        TigrisAPIError: If the gateway answers with an error

    Usage:
        page = list_buckets(s3_client, include_type=True, include_regions=True)
        for bucket in page['buckets']:
            print(bucket['name'], bucket['snapshot_enabled'], bucket['regions'])
    """
    query: dict[str, Optional[str]] = {}
    for flag, name in (
        (include_regions, "IncludeRegionsInfo"),
        (include_type, "IncludeTypeInfo"),
        (include_visibility, "IncludeVisibility"),
        (include_fork_info, "IncludeForkInfo"),
        (include_owner, "IncludeOwnerInfo"),
        (include_stats, "IncludeStats"),
        (only_forks, "OnlyForked"),
    ):
        if flag:
            query[name] = "true"
    if owned_by:
        query["OwnedBy"] = owned_by
    query.update(paging_params(max_buckets, continuation_token))

    response = signed_request(
        s3_client, "GET", "", query=query, headers={"Accept": "application/json"}
    )
    raw = response.json()
    page = parse_listing_page(raw)
    if include_regions:
        for bucket in page["buckets"]:
            if bucket["regions"] is None:
                bucket["regions"] = []
    page["stats"] = _parse_stats(raw.get("Stats")) if include_stats else None
    return page


def _parse_stats(stats: Optional[dict[str, Any]]) -> dict[str, Any]:
    stats = stats or {}
    return {key: stats.get(wire, 0) for key, wire in _STATS_FIELDS.items()}
