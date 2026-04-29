"""Configure Tigris object-event webhook notifications on a bucket.

Tigris exposes object-event webhooks via a bucket-settings extension that
isn't part of the S3 API: a SigV4-signed ``PATCH /{bucket}`` with a JSON
body. This module is the public surface for that feature; lower-level
helpers in :mod:`tigris_boto3_ext.agent_kit` (``setup_coordination`` /
``teardown_coordination``) are thin wrappers around it.

When an object is created, deleted, or modified Tigris will POST a
notification to the configured webhook URL.
"""

import hashlib
import json
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Optional

import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


_pool = urllib3.PoolManager(num_pools=2, maxsize=4)


class ObjectNotificationsError(Exception):
    """Raised when configuring Tigris object notifications fails."""

    def __init__(self, message: str, status_code: int, body: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def set_object_notifications(
    s3_client: S3Client,
    bucket: str,
    *,
    webhook_url: str,
    event_filter: Optional[str] = None,
    auth_token: Optional[str] = None,
    auth_username: Optional[str] = None,
    auth_password: Optional[str] = None,
) -> None:
    """Enable object-event webhook notifications on a bucket.

    Args:
        s3_client: boto3 S3 client; endpoint and credentials are reused.
        bucket: Bucket to configure.
        webhook_url: Destination URL (http or https) Tigris will POST to.
        event_filter: Optional Tigris filter expression (e.g. one matching
            object keys under a prefix via REGEXP).
        auth_token: Bearer token Tigris will send. Mutually exclusive with
            basic auth.
        auth_username: Basic-auth username; must be paired with
            ``auth_password``.
        auth_password: Basic-auth password; must be paired with
            ``auth_username``.

    Raises:
        ValueError: For invalid arguments (empty/non-http URL, mixed auth
            modes, partial basic-auth credentials).
        ObjectNotificationsError: On non-2xx responses from Tigris.
    """
    _validate_webhook_url(webhook_url)
    if auth_token is not None and (
        auth_username is not None or auth_password is not None
    ):
        msg = "auth_token cannot be combined with auth_username/auth_password"
        raise ValueError(msg)
    if (auth_username is None) != (auth_password is None):
        msg = "auth_username and auth_password must be provided together"
        raise ValueError(msg)

    notification: dict[str, Any] = {
        "enabled": True,
        "web_hook": webhook_url,
        "filter": event_filter or "",
    }
    if auth_token is not None:
        notification["auth"] = {"token": auth_token}
    elif auth_username is not None and auth_password is not None:
        notification["auth"] = {
            "basic_user": auth_username,
            "basic_pass": auth_password,
        }

    _patch_bucket(s3_client, bucket, {"object_notifications": notification})


def clear_object_notifications(s3_client: S3Client, bucket: str) -> None:
    """Disable webhook notifications on a bucket."""
    _patch_bucket(s3_client, bucket, {"object_notifications": {}})


def _validate_webhook_url(url: str) -> None:
    if not url:
        msg = "webhook_url is required"
        raise ValueError(msg)
    if not url.startswith(("http://", "https://")):
        msg = f"webhook_url must use http or https, got {url!r}"
        raise ValueError(msg)


def _patch_bucket(
    s3_client: S3Client,
    bucket: str,
    body: dict[str, Any],
) -> None:
    """SigV4-sign and send a ``PATCH /{bucket}`` with a JSON body."""
    if not bucket:
        msg = "bucket is required"
        raise ValueError(msg)

    endpoint = s3_client.meta.endpoint_url
    credentials = s3_client._request_signer._credentials.get_frozen_credentials()  # type: ignore[attr-defined]  # noqa: SLF001
    region = s3_client.meta.region_name or "auto"

    url = f"{endpoint.rstrip('/')}/{bucket}"
    payload = json.dumps(body)

    headers = {
        "Content-Type": "application/json",
        "X-Amz-Content-Sha256": hashlib.sha256(payload.encode()).hexdigest(),
    }

    request = AWSRequest(method="PATCH", url=url, data=payload, headers=headers)
    SigV4Auth(credentials, "s3", region).add_auth(request)
    prepared = request.prepare()

    response = _pool.urlopen(
        prepared.method,
        prepared.url,
        body=prepared.body,
        headers=dict(prepared.headers),
    )

    if response.status >= HTTPStatus.BAD_REQUEST:
        raw = response.data or b""
        text = raw.decode("utf-8", errors="replace") if raw else ""
        msg = f"Object-notifications request failed (HTTP {response.status}): {text}"
        raise ObjectNotificationsError(msg, status_code=response.status, body=text)
