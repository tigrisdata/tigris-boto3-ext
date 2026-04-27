"""Direct HTTP support for Tigris-specific REST endpoints.

Some Tigris features (TTL/lifecycle, webhook notifications) live behind a
``PATCH /{bucket}`` JSON API rather than the S3-compatible header-injection
path that the rest of this library uses. This module mirrors the pattern in
``bundle.py``: extract endpoint and credentials from the boto3 client, sign
the request with SigV4, and send it via urllib3.
"""

import hashlib
import json
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


_rest_pool = urllib3.PoolManager(
    num_pools=4,
    maxsize=8,
)


class TigrisRestError(Exception):
    """Raised when a Tigris REST request fails."""

    def __init__(self, message: str, status_code: int, body: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def patch_bucket_settings(
    s3_client: S3Client,
    bucket: str,
    body: dict[str, Any],
) -> dict[str, Any]:
    """Send a ``PATCH /{bucket}`` request with a JSON settings body.

    This is the transport for Tigris-specific bucket settings such as
    ``lifecycle_rules`` (TTL) and ``object_notifications`` (webhooks).

    Args:
        s3_client: boto3 S3 client. Endpoint and credentials are reused.
        bucket: Bucket name to update.
        body: JSON body. Examples::

            {"lifecycle_rules": [{"id": "...", "expiration": {"days": 1, "enabled": True}, "status": 1}]}
            {"object_notifications": {"enabled": True, "web_hook": "https://...", "filter": ""}}
            {"object_notifications": {}}  # clear notifications

    Returns:
        Parsed JSON response, or an empty dict if no body.

    Raises:
        ValueError: If bucket is empty.
        TigrisRestError: If the HTTP request returns >= 400.
    """
    if not bucket:
        msg = "bucket is required"
        raise ValueError(msg)

    endpoint = s3_client.meta.endpoint_url
    credentials = s3_client._request_signer._credentials.get_frozen_credentials()  # type: ignore[attr-defined]  # noqa: SLF001
    region = s3_client.meta.region_name or "auto"

    url = f"{endpoint.rstrip('/')}/{bucket}"
    payload = json.dumps(body)

    # X-Amz-Content-Sha256 must be present in the headers passed to SigV4 so it
    # is included in the canonical request and signed; otherwise Tigris rejects
    # the PATCH with SignatureDoesNotMatch. Mirrors the bundle.py pattern.
    headers = {
        "Content-Type": "application/json",
        "X-Amz-Content-Sha256": hashlib.sha256(payload.encode()).hexdigest(),
    }

    request = AWSRequest(method="PATCH", url=url, data=payload, headers=headers)
    SigV4Auth(credentials, "s3", region).add_auth(request)
    prepared = request.prepare()

    response = _rest_pool.urlopen(
        prepared.method,
        prepared.url,
        body=prepared.body,
        headers=dict(prepared.headers),
    )

    raw_body = response.data or b""

    if response.status >= HTTPStatus.BAD_REQUEST:
        text = raw_body.decode("utf-8", errors="replace") if raw_body else ""
        msg = f"Tigris REST request failed (HTTP {response.status}): {text}"
        raise TigrisRestError(msg, status_code=response.status, body=text)

    if not raw_body:
        return {}

    try:
        parsed = json.loads(raw_body)
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}
