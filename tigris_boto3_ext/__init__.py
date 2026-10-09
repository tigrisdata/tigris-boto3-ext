"""
Tigris boto3 Extensions - Extend boto3 S3 client with Tigris-specific features.

This library provides context managers, decorators, and helper functions to enable
Tigris-specific features like snapshots and bucket forking while maintaining full
boto3 compatibility.
"""

from .buckets.delete import (
    TigrisSoftDeleteEnabled,
    create_soft_delete_bucket,
    force_delete_bucket,
    soft_delete_enabled,
)
from .buckets.forks import TigrisFork, create_fork, forked_from
from .buckets.info import get_bucket_info, has_snapshot_enabled
from .buckets.snapshots import (
    TigrisSnapshot,
    TigrisSnapshotEnabled,
    create_snapshot,
    create_snapshot_bucket,
    delete_snapshot,
    get_snapshot_version,
    list_snapshots,
    snapshot_enabled,
    with_snapshot,
)
from .objects.bundle import (
    BUNDLE_COMPRESSION_GZIP,
    BUNDLE_COMPRESSION_NONE,
    BUNDLE_COMPRESSION_ZSTD,
    BUNDLE_ON_ERROR_FAIL,
    BUNDLE_ON_ERROR_SKIP,
    MAX_BUNDLE_KEYS,
    BundleError,
    BundleResponse,
    bundle_objects,
)
from .objects.delete import (
    TigrisSoftDeleteView,
    list_deleted_object_versions,
    list_deleted_objects,
    purge_deleted_object,
    restore_deleted_object,
    with_soft_delete_view,
)
from .objects.rename import TigrisRename, rename_object, with_rename
from .objects.snapshots import (
    get_object_from_snapshot,
    head_object_from_snapshot,
    list_objects_from_snapshot,
)

__version__ = "0.4.0"

__all__ = [
    # Bucket info
    "get_bucket_info",
    "has_snapshot_enabled",
    # Bucket deletion
    "TigrisSoftDeleteEnabled",
    "soft_delete_enabled",
    "create_soft_delete_bucket",
    "force_delete_bucket",
    # Bucket snapshots
    "TigrisSnapshotEnabled",
    "TigrisSnapshot",
    "snapshot_enabled",
    "with_snapshot",
    "create_snapshot_bucket",
    "create_snapshot",
    "get_snapshot_version",
    "list_snapshots",
    "delete_snapshot",
    # Bucket forks
    "TigrisFork",
    "forked_from",
    "create_fork",
    # Object rename
    "TigrisRename",
    "with_rename",
    "rename_object",
    # Object snapshots
    "get_object_from_snapshot",
    "list_objects_from_snapshot",
    "head_object_from_snapshot",
    # Object deletion
    "TigrisSoftDeleteView",
    "with_soft_delete_view",
    "list_deleted_objects",
    "list_deleted_object_versions",
    "purge_deleted_object",
    "restore_deleted_object",
    # Bundle API
    "bundle_objects",
    "BundleError",
    "BundleResponse",
    "MAX_BUNDLE_KEYS",
    "BUNDLE_COMPRESSION_NONE",
    "BUNDLE_COMPRESSION_GZIP",
    "BUNDLE_COMPRESSION_ZSTD",
    "BUNDLE_ON_ERROR_SKIP",
    "BUNDLE_ON_ERROR_FAIL",
]
