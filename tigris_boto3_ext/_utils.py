"""Small helpers shared across resources."""

from datetime import datetime, timezone
from typing import Any, Optional

from botocore.utils import parse_timestamp as _parse_timestamp


def parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse a gateway timestamp into an aware UTC datetime.

    Returns None for missing or unparseable values.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = _parse_timestamp(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
