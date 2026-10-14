"""Signed requests to Tigris endpoints that are not S3 operations."""

import hashlib
import html
import json
import re
from dataclasses import dataclass, field
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Optional, Union
from urllib.parse import quote

from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import NoCredentialsError

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

_XML_FIELD = re.compile(r"<(Code|Message|RequestId)>(.*?)</\1>", re.S)


class TigrisAPIError(Exception):
    """
    A Tigris endpoint answered with an error status.

    Attributes:
        status_code: The HTTP status code.
        code: The error code from the body, such as ``NoSuchBucket``, when the
            body is a Tigris or S3-style error document; otherwise None.
        message: The error message from the body, or the raw body when it is
            not an error document.
        request_id: The server-side request id, for support tickets. May be None.
        body: The raw response body.
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        body: str,
        code: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.body = body
        self.code = code
        self.request_id = request_id

    @classmethod
    def from_response(
        cls, status_code: int, body: str, headers: dict[str, str]
    ) -> "TigrisAPIError":
        """Build the error from a response, reading a Tigris JSON or S3 XML error document."""
        code, message, request_id = _parse_error_document(body)
        request_id = request_id or headers.get("x-amz-request-id")
        summary = f"{code}: {message}" if code else (message or f"HTTP {status_code}")
        return cls(
            f"Tigris request failed (HTTP {status_code}): {summary}",
            status_code=status_code,
            body=body,
            code=code,
            request_id=request_id,
        )


@dataclass
class TigrisResponse:
    """A fully read response from a Tigris endpoint."""

    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""

    def json(self) -> Any:
        """Decode the body as JSON."""
        return json.loads(self.body)


def signed_request(
    s3_client: S3Client,
    method: str,
    path: str,
    *,
    query: Optional[Union[str, dict[str, Optional[str]]]] = None,
    headers: Optional[dict[str, str]] = None,
    body: Union[bytes, str, None] = None,
    stream: bool = False,
) -> Any:
    """
    Send a SigV4-signed request to Tigris through a boto3 client's endpoint.

    The endpoint, region and credentials are taken from the client, and the
    request goes out through the client's own HTTP session, so it is
    authenticated and transported exactly like the client's S3 calls: same
    proxies, CA bundle, client certificate and timeouts.

    Args:
        s3_client: boto3 S3 client instance
        method: HTTP method
        path: Path relative to the endpoint, such as ``my-bucket`` or ``""``
        query: Query string without the leading ``?``, or a dict whose None
            values are bare flags (``{"metadata": None, "with-size": "true"}``
            becomes ``metadata&with-size=true``)
        headers: Extra request headers
        body: Request body; ``str`` is sent as UTF-8
        stream: Return the raw response without reading the body, for streaming
            payloads such as the bundle archive

    Returns:
        A ``TigrisResponse`` with the body read, or the raw ``urllib3.HTTPResponse``
        when ``stream`` is True

    Raises:
        TigrisAPIError: On any 4xx or 5xx response
        botocore.exceptions.NoCredentialsError: If the client has no credentials
        botocore.exceptions.HTTPClientError: On connection failures, as for S3 calls
    """
    endpoint = s3_client.meta.endpoint_url
    credentials = s3_client._request_signer._credentials  # type: ignore[attr-defined]  # noqa: SLF001
    if credentials is None:
        raise NoCredentialsError
    frozen = credentials.get_frozen_credentials()
    region = s3_client.meta.region_name or "auto"

    url = f"{endpoint.rstrip('/')}/{path.lstrip('/')}"
    query_string = _build_query(query)
    if query_string:
        url = f"{url}?{query_string}"

    payload = body.encode() if isinstance(body, str) else (body or b"")
    request_headers = {
        "X-Amz-Content-Sha256": hashlib.sha256(payload).hexdigest(),
        **(headers or {}),
    }

    request = AWSRequest(
        method=method,
        url=url,
        data=payload,
        headers=request_headers,
        stream_output=stream,
    )
    SigV4Auth(frozen, "s3", region).add_auth(request)
    prepared = request.prepare()

    # The client's own session carries its proxies, CA bundle, certificate
    # and timeout configuration, so these calls behave like its S3 calls.
    response = s3_client._endpoint.http_session.send(prepared)  # type: ignore[attr-defined]  # noqa: SLF001

    if response.status_code >= HTTPStatus.BAD_REQUEST:
        try:
            error_body = response.content.decode("utf-8", errors="replace")
        except Exception:
            error_body = ""
        finally:
            response.raw.close()
        raise TigrisAPIError.from_response(
            response.status_code, error_body, _lower_keys(response.headers)
        )

    if stream:
        return response.raw
    return TigrisResponse(
        status_code=response.status_code,
        headers=_lower_keys(response.headers),
        body=response.content,
    )


def _build_query(query: Optional[Union[str, dict[str, Optional[str]]]]) -> str:
    if not query:
        return ""
    if isinstance(query, str):
        return query
    return "&".join(
        quote(name, safe="")
        if value is None
        else f"{quote(name, safe='')}={quote(str(value), safe='')}"
        for name, value in query.items()
    )


def _lower_keys(headers: Any) -> dict[str, str]:
    return {key.lower(): value for key, value in dict(headers).items()}


def _parse_error_document(
    body: str,
) -> tuple[Optional[str], str, Optional[str]]:
    """Return (code, message, request_id) from a Tigris JSON or S3 XML error body."""
    text = body.strip()
    if text.startswith("{"):
        try:
            document = json.loads(text)
        except ValueError:
            document = None
        if isinstance(document, dict):
            return (
                _optional_str(document.get("Code")),
                _optional_str(document.get("Message")) or text,
                _optional_str(document.get("RequestId")),
            )
    if text.startswith("<"):
        fields = {
            name: html.unescape(value.strip())
            for name, value in _XML_FIELD.findall(text)
        }
        if "Code" in fields or "Message" in fields:
            return (
                fields.get("Code"),
                fields.get("Message", text),
                fields.get("RequestId"),
            )
    return None, text, None


def _optional_str(value: Any) -> Optional[str]:
    return str(value) if value not in (None, "") else None
