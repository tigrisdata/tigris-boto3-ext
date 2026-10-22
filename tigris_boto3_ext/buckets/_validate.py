"""Validation of bucket settings shared by creation and update."""

from collections.abc import Sequence
from typing import Union
from urllib.parse import urlsplit

STORAGE_CLASSES = ("STANDARD", "STANDARD_IA", "GLACIER", "GLACIER_IR")
ACCESS_VALUES = ("public", "private")


def storage_class(value: str, name: str = "default_storage_class") -> str:
    if value not in STORAGE_CLASSES:
        msg = f"{name} must be one of {', '.join(STORAGE_CLASSES)}, got {value!r}"
        raise ValueError(msg)
    return value


def access(value: str) -> str:
    if value not in ACCESS_VALUES:
        msg = f"access must be 'public' or 'private', got {value!r}"
        raise ValueError(msg)
    return value


def regions(value: Union[str, Sequence[str]], *, allow_empty: bool = False) -> str:
    """Join region codes into the comma-separated wire form; empty means global."""
    names = [value] if isinstance(value, str) else list(value)
    cleaned = [name.strip() for name in names if name and name.strip()]
    if not cleaned and not allow_empty:
        msg = "regions must name at least one region"
        raise ValueError(msg)
    return ",".join(cleaned)


def flag(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        msg = f"{name} must be a bool"
        raise TypeError(msg)
    return value


def https_url(value: str, name: str) -> str:
    parts = urlsplit(value)
    if parts.scheme != "https" or not parts.hostname:
        msg = f"{name} must be an https:// URL with a host, got {value!r}"
        raise ValueError(msg)
    return value
