"""Parsers for the JSON payloads of Tigris endpoints outside the S3 API."""

from datetime import datetime, timezone
from typing import Any, Optional


def parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse a Tigris RFC 3339 timestamp, which may carry nanoseconds and a ``Z``.

    Returns None for missing or unparseable values.
    """
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    if "." in text:
        head, _, rest = text.partition(".")
        digits = ""
        while rest and rest[0].isdigit():
            digits += rest[0]
            rest = rest[1:]
        text = f"{head}.{digits[:6].ljust(6, '0')}{rest}"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def parse_fork_info(fork_info: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Normalize a ``ForkInfo`` block into ``is_fork_parent`` and ``fork_parents``.

    The gateway encodes a named snapshot as ``<version>; name=<name>``, the same
    pseudo-bucket form ``list_snapshots`` returns; an unnamed one is bare.
    ``Parents`` may be null on buckets that are not forks.
    """
    fork_info = fork_info or {}
    parents = []
    for parent in fork_info.get("Parents") or []:
        version, _, name = str(parent.get("Snapshot") or "").partition("; name=")
        parents.append(
            {
                "bucket": parent.get("BucketName"),
                "snapshot": version or None,
                "snapshot_name": name or None,
                "forked_at": parse_timestamp(parent.get("ForkCreatedAt")),
                "snapshot_created_at": parse_timestamp(parent.get("SnapshotCreatedAt")),
            }
        )
    return {
        "is_fork_parent": bool(fork_info.get("HasChildren", False)),
        "fork_parents": parents,
    }


def parse_soft_delete(info: Optional[dict[str, Any]]) -> dict[str, Any]:
    """Normalize a ``SoftDeleteInfo`` (listing) or ``soft_delete`` (metadata) block."""
    info = info or {}
    enabled = bool(info.get("Enabled", info.get("enabled", False)))
    return {
        "enabled": enabled,
        "retention_days": info.get("RetentionDays", info.get("retention_days")),
    }


def parse_bucket_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Normalize one entry of a ``GET /`` bucket listing.

    Fields the listing only carries when the matching ``Include*`` flag was sent
    come back as None when absent: ``snapshot_enabled``, ``visibility``,
    ``regions``, ``is_fork_parent`` / ``fork_parents`` and ``owner``. Callers
    that asked for regions turn a missing ``Regions`` into ``[]`` (global).
    """
    fork = parse_fork_info(entry["ForkInfo"]) if "ForkInfo" in entry else None
    visibility = entry.get("Visibility")
    owner_info = entry.get("OwnerInfo")
    regions = entry.get("Regions")
    return {
        "name": entry.get("Name"),
        "created_at": parse_timestamp(
            entry.get("InitialCreatedDate") or entry.get("CreationDate")
        ),
        "modified_at": parse_timestamp(entry.get("CreationDate")),
        "snapshot_enabled": (entry["Type"] == "Snapshot") if "Type" in entry else None,
        "visibility": (
            ("public" if visibility.get("IsPublic") else "private")
            if isinstance(visibility, dict)
            else None
        ),
        "regions": (
            [region.strip() for region in regions.split(",") if region.strip()]
            if isinstance(regions, str)
            else None
        ),
        "is_fork_parent": fork["is_fork_parent"] if fork else None,
        "fork_parents": fork["fork_parents"] if fork else None,
        "soft_delete": parse_soft_delete(entry.get("SoftDeleteInfo")),
        "owner": (
            {
                "user_id": owner_info.get("OwnerUserId"),
                "human_user_id": owner_info.get("OwnerHumanUserId"),
            }
            if isinstance(owner_info, dict)
            else None
        ),
    }


def listing_entries(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """The ``Buckets.Bucket`` list of a ``GET /`` response; a single entry can arrive unwrapped."""
    entries = (raw.get("Buckets") or {}).get("Bucket") or []
    if isinstance(entries, dict):
        entries = [entries]
    return list(entries)


def parse_listing_page(raw: dict[str, Any]) -> dict[str, Any]:
    """The normalized shape shared by every ``GET /`` bucket listing."""
    owner = raw.get("Owner") or {}
    return {
        "buckets": [parse_bucket_entry(entry) for entry in listing_entries(raw)],
        "continuation_token": raw.get("ContinuationToken") or None,
        "owner": {"id": owner.get("ID"), "display_name": owner.get("DisplayName")},
        "raw": raw,
    }
