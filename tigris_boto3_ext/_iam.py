"""Tigris IAM HTTP transport for managing access keys.

Tigris IAM lives at a separate endpoint from the S3 API and uses an
AWS-IAM-compatible POST + form-encoded request format with custom Tigris
extensions for bucket-scoped access keys.

This module provides the minimum needed to support agent-kit's
``credentials_role`` option — creating and deleting an access key — by
posting to ``https://iam.storageapi.dev/`` (override with the
``TIGRIS_IAM_ENDPOINT`` env var) signed with the boto3 client's credentials
under the ``iam`` SigV4 service.
"""

import hashlib
import json
import os
import uuid
import xml.etree.ElementTree as ET
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Optional
from urllib.parse import urlencode

import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object


_iam_pool = urllib3.PoolManager(num_pools=2, maxsize=4)


DEFAULT_IAM_ENDPOINT = "https://iam.storageapi.dev"


class TigrisIAMError(Exception):
    """Raised when a Tigris IAM request fails."""

    def __init__(self, message: str, status_code: int, body: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


def _iam_endpoint() -> str:
    return os.environ.get("TIGRIS_IAM_ENDPOINT") or DEFAULT_IAM_ENDPOINT


def _post_form(
    s3_client: S3Client,
    path_and_query: str,
    form: dict[str, str],
) -> bytes:
    """Sign and POST a form-encoded body to the IAM endpoint. Returns raw bytes.

    `path_and_query` is everything after the host (e.g. ``"/?Action=DeleteAccessKey"``).
    """
    credentials = s3_client._request_signer._credentials.get_frozen_credentials()  # type: ignore[attr-defined]  # noqa: SLF001
    region = s3_client.meta.region_name or "auto"

    url = f"{_iam_endpoint().rstrip('/')}{path_and_query}"
    payload = urlencode(form)

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json",
        "X-Amz-Content-Sha256": hashlib.sha256(payload.encode()).hexdigest(),
    }

    request = AWSRequest(method="POST", url=url, data=payload, headers=headers)
    SigV4Auth(credentials, "iam", region).add_auth(request)
    prepared = request.prepare()

    response = _iam_pool.urlopen(
        prepared.method,
        prepared.url,
        body=prepared.body,
        headers=dict(prepared.headers),
    )

    raw = response.data or b""

    if response.status >= HTTPStatus.BAD_REQUEST:
        text = raw.decode("utf-8", errors="replace") if raw else ""
        msg = f"Tigris IAM request failed (HTTP {response.status}): {text}"
        raise TigrisIAMError(msg, status_code=response.status, body=text)

    return raw


def create_access_key_with_buckets_role(
    s3_client: S3Client,
    name: str,
    buckets_role: list[dict[str, str]],
) -> dict[str, Any]:
    """Create a Tigris access key scoped to one or more buckets.

    Args:
        s3_client: boto3 S3 client. Credentials and region are reused.
        name: Display name for the new access key.
        buckets_role: List of ``{"bucket": str, "role": "Editor" | "ReadOnly"}``.

    Returns:
        ``{"access_key_id": str, "secret_access_key": str, "name": str}``.

    Raises:
        TigrisIAMError: On non-2xx responses.
        RuntimeError: If the response shape is missing the access key fields.
    """
    req_body = {
        "req_uuid": str(uuid.uuid4()),
        "name": name,
        "buckets_role": buckets_role,
    }
    raw = _post_form(
        s3_client,
        "/?Action=CreateAccessKeyWithBucketsRole",
        {"Req": json.dumps(req_body)},
    )

    access_key = _parse_create_access_key_response(raw)
    if access_key is None:
        msg = f"Tigris IAM CreateAccessKey returned an unexpected response: {raw!r}"
        raise RuntimeError(msg)
    return access_key


def delete_access_key(s3_client: S3Client, access_key_id: str) -> None:
    """Delete a Tigris access key by id.

    Mirrors the AWS IAM ``DeleteAccessKey`` action.
    """
    _post_form(
        s3_client,
        "/?Action=DeleteAccessKey",
        {
            "Action": "DeleteAccessKey",
            "Version": "2010-05-08",
            "AccessKeyId": access_key_id,
        },
    )


def _parse_create_access_key_response(raw: bytes) -> Optional[dict[str, Any]]:
    """Parse the CreateAccessKey response (JSON or AWS-style XML)."""
    if not raw:
        return None

    text = raw.decode("utf-8", errors="replace").strip()
    fields = (
        _parse_create_access_key_json(text)
        if text.startswith("{")
        else _parse_create_access_key_xml(text)
    )
    if fields is None:
        return None
    access_key_id, secret, user_name = fields
    if not access_key_id or not secret:
        return None
    return {
        "access_key_id": access_key_id,
        "secret_access_key": secret,
        "name": user_name,
    }


def _parse_create_access_key_json(
    text: str,
) -> Optional[tuple[str, str, str]]:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return None
    access_key = data.get("CreateAccessKeyResult", {}).get("AccessKey")
    if not isinstance(access_key, dict):
        return None
    return (
        access_key.get("AccessKeyId", ""),
        access_key.get("SecretAccessKey", ""),
        access_key.get("UserName", ""),
    )


def _parse_create_access_key_xml(
    text: str,
) -> Optional[tuple[str, str, str]]:
    # Tigris IAM endpoint is authenticated; XML is from a trusted source.
    try:
        root = ET.fromstring(text)  # noqa: S314
    except ET.ParseError:
        return None
    tag_to_text: dict[str, str] = {}
    for elem in root.iter():
        local = elem.tag.split("}", 1)[-1]
        if elem.text is not None:
            tag_to_text[local] = elem.text
    return (
        tag_to_text.get("AccessKeyId", ""),
        tag_to_text.get("SecretAccessKey", ""),
        tag_to_text.get("UserName", ""),
    )
