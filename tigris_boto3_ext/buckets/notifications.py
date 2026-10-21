"""Object notifications: deliver a bucket's object events to a webhook."""

import json
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

from tigris_boto3_ext._transport import signed_request
from tigris_boto3_ext.buckets import _validate


def set_bucket_notifications(
    s3_client: S3Client,
    bucket_name: str,
    url: Optional[str],
    *,
    enabled: bool = True,
    event_filter: Optional[str] = None,
    token: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
) -> dict[str, Any]:
    """
    Send a bucket's object events to a webhook, or stop sending them.

    Tigris POSTs a JSON payload to ``url`` when objects in the bucket are
    created, deleted, renamed or transitioned. This uses Tigris's
    ``PATCH /{bucket}`` endpoint, which is not an S3 operation, so the result
    is a plain dict. Each call replaces the whole notification configuration;
    ``url=None`` removes it. Read it back with ``get_bucket_metadata`` under
    ``notifications``; credentials come back masked.

    Args:
        s3_client: boto3 S3 client instance
        bucket_name: Name of the bucket
        url: ``https://`` endpoint that receives the events, or None to remove
            the configuration
        enabled: False keeps the configuration but pauses delivery
        event_filter: Which events are delivered, in Tigris's SQL-like filter
            syntax, for example ``WHERE `Event-Type` = "OBJECT_DELETED"`` or
            ``WHERE `key` REGEXP "^images/"``; None delivers every event
        token: Sent as ``Authorization: Bearer <token>``
        username: Sent with ``password`` as HTTP basic authentication
        password: See ``username``

    Returns:
        {'name': bucket_name, 'request_id': str or None, 'raw': dict}

    Raises:
        ValueError: If ``bucket_name`` is empty, ``url`` is not an ``https://``
            URL with a host, ``token`` is combined with ``username`` or
            ``password``, only one of ``username`` and ``password`` is given,
            a credential is empty, or other settings are passed with ``url=None``
        TypeError: If ``enabled`` is not a bool
        TigrisAPIError: If the gateway answers with an error, for example
            ``NoSuchBucket`` (404) or an invalid filter (400 ``InvalidArgument``)

    Usage:
        set_bucket_notifications(s3_client, 'uploads', 'https://api.example.com/hooks/tigris', token='s3cr3t')
        set_bucket_notifications(
            s3_client, 'uploads', 'https://api.example.com/hooks/tigris',
            event_filter='WHERE `Event-Type` = "OBJECT_DELETED"',
        )
        set_bucket_notifications(s3_client, 'uploads', None)
    """
    if not bucket_name:
        msg = "bucket_name is required"
        raise ValueError(msg)

    settings: dict[str, Any] = {}
    if url is None:
        if any(
            value is not None for value in (event_filter, token, username, password)
        ):
            msg = "url=None removes the configuration; pass no other settings with it"
            raise ValueError(msg)
    else:
        settings["enabled"] = _validate.flag(enabled, "enabled")
        settings["web_hook"] = _webhook(url)
        if event_filter is not None:
            settings["filter"] = event_filter
        auth = _auth(token, username, password)
        if auth is not None:
            settings["auth"] = auth

    response = signed_request(
        s3_client,
        "PATCH",
        bucket_name,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        body=json.dumps({"object_notifications": settings}),
    )
    raw = response.json() if response.body.strip() else {}
    return {
        "name": bucket_name,
        "request_id": response.headers.get("x-amz-request-id"),
        "raw": raw,
    }


def _webhook(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname:
        msg = f"url must be an https:// URL with a host, got {url!r}"
        raise ValueError(msg)
    return url


def _auth(
    token: Optional[str], username: Optional[str], password: Optional[str]
) -> Optional[dict[str, str]]:
    if token is not None and (username is not None or password is not None):
        msg = "pass either token or username and password, not both"
        raise ValueError(msg)
    if (username is None) != (password is None):
        msg = "username and password must be given together"
        raise ValueError(msg)
    if token is not None:
        if not token:
            msg = "token must not be empty"
            raise ValueError(msg)
        return {"token": token}
    if username is not None and password is not None:
        if not username or not password:
            msg = "username and password must not be empty"
            raise ValueError(msg)
        return {"basic_user": username, "basic_pass": password}
    return None
