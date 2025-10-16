"""
Tigris boto3 Extensions - Extend boto3 S3 client with Tigris-specific features.

This library provides context managers, decorators, and helper functions to enable
Tigris-specific features like snapshots and bucket forking while maintaining full
boto3 compatibility.
"""

from .context_managers import TigrisFork, TigrisSnapshot, TigrisSnapshotEnabled
from .decorators import forked_from, snapshot_enabled, with_snapshot
from .helpers import (
    create_fork,
    create_snapshot,
    create_snapshot_bucket,
    get_bucket_info,
    get_object_from_snapshot,
    get_snapshot_version,
    has_snapshot_enabled,
    head_object_from_snapshot,
    list_objects_from_snapshot,
    list_snapshots,
)

__version__ = "0.1.0"

__all__ = [
    # Context Managers
    "TigrisSnapshotEnabled",
    "TigrisSnapshot",
    "TigrisFork",
    # Decorators
    "snapshot_enabled",
    "with_snapshot",
    "forked_from",
    # Helper Functions
    "create_snapshot_bucket",
    "create_snapshot",
    "get_snapshot_version",
    "list_snapshots",
    "create_fork",
    "get_object_from_snapshot",
    "list_objects_from_snapshot",
    "head_object_from_snapshot",
    "has_snapshot_enabled",
    "get_bucket_info",
]
