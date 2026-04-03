"""Bundle API support for fetching multiple objects as a streaming tar archive."""

import hashlib
import json
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, Optional

import urllib3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

_bundle_pool = urllib3.PoolManager()

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client
else:
    S3Client = object

# Bundle API constants
BUNDLE_COMPRESSION_NONE = "none"
BUNDLE_COMPRESSION_GZIP = "gzip"
BUNDLE_COMPRESSION_ZSTD = "zstd"

BUNDLE_ON_ERROR_SKIP = "skip"
BUNDLE_ON_ERROR_FAIL = "fail"


class BundleResponse:
    """Response from a bundle_objects request.

    The body is a streaming tar archive. Use tarfile to extract entries:

        import tarfile

        response = bundle_objects(s3_client, "my-bucket", keys)
        with tarfile.open(fileobj=response, mode="r|") as tar:
            for member in tar:
                f = tar.extractfile(member)
                if f is not None:
                    data = f.read()

    If compression was requested, wrap with the appropriate decompressor:

        import gzip
        import tarfile

        response = bundle_objects(s3_client, "my-bucket", keys, compression="gzip")
        with gzip.open(response, "rb") as gz:
            with tarfile.open(fileobj=gz, mode="r|") as tar:
                for member in tar:
                    ...

    Attributes:
        body: The raw streaming response body (file-like object).
        content_type: The response Content-Type header.
        status_code: The HTTP status code.
        headers: All response headers.
    """

    def __init__(
        self,
        body: Any,
        content_type: str,
        status_code: int,
        headers: dict[str, str],
    ) -> None:
        self.body = body
        self.content_type = content_type
        self.status_code = status_code
        self.headers = headers

    def read(self, amt: Optional[int] = None) -> bytes:
        """Read from the response body. Makes BundleResponse file-like."""
        return self.body.read(amt)  # type: ignore[no-any-return]

    def close(self) -> None:
        """Close the response body."""
        self.body.close()

    def __enter__(self) -> "BundleResponse":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


def bundle_objects(
    s3_client: S3Client,
    bucket: str,
    keys: list[str],
    *,
    compression: str = BUNDLE_COMPRESSION_NONE,
    on_error: str = BUNDLE_ON_ERROR_SKIP,
) -> BundleResponse:
    """Fetch multiple objects from a bucket as a streaming tar archive.

    This is a Tigris extension to the S3 API, designed for ML training workloads
    that need to fetch thousands of objects per batch without per-object HTTP
    overhead.

    Args:
        s3_client: boto3 S3 client instance.
        bucket: Name of the bucket containing the objects.
        keys: List of object keys to include (max 5,000).
        compression: Compression algorithm — "none" (default), "gzip", or "zstd".
        on_error: Error mode — "skip" (default) or "fail".

    Returns:
        BundleResponse with a streaming body that can be passed to tarfile.open().

    Raises:
        ValueError: If bucket or keys are empty, or if invalid options are provided.
        Exception: If the HTTP request fails.

    Usage:
        import tarfile
        from tigris_boto3_ext import bundle_objects

        response = bundle_objects(s3_client, "my-bucket", [
            "dataset/train/img_001.jpg",
            "dataset/train/img_002.jpg",
        ])

        with tarfile.open(fileobj=response, mode="r|") as tar:
            for member in tar:
                if member.name == "__bundle_errors.json":
                    continue
                f = tar.extractfile(member)
                if f is not None:
                    image_bytes = f.read()
    """
    if not bucket:
        msg = "bucket is required"
        raise ValueError(msg)
    if not keys:
        msg = "at least one key is required"
        raise ValueError(msg)
    if compression not in (
        BUNDLE_COMPRESSION_NONE,
        BUNDLE_COMPRESSION_GZIP,
        BUNDLE_COMPRESSION_ZSTD,
    ):
        msg = (
            f"invalid compression: {compression!r} (must be 'none', 'gzip', or 'zstd')"
        )
        raise ValueError(msg)
    if on_error not in (BUNDLE_ON_ERROR_SKIP, BUNDLE_ON_ERROR_FAIL):
        msg = f"invalid on_error: {on_error!r} (must be 'skip' or 'fail')"
        raise ValueError(msg)

    # Extract endpoint and credentials from the boto3 client.
    endpoint = s3_client.meta.endpoint_url
    credentials = s3_client._request_signer._credentials  # type: ignore[attr-defined]  # noqa: SLF001
    region = s3_client.meta.region_name or "auto"

    url = f"{endpoint.rstrip('/')}/{bucket}?bundle"

    body = json.dumps({"keys": keys})

    headers = {
        "Content-Type": "application/json",
        "X-Tigris-Bundle-Format": "tar",
        "X-Tigris-Bundle-Compression": compression,
        "X-Tigris-Bundle-On-Error": on_error,
        "X-Amz-Content-Sha256": hashlib.sha256(body.encode()).hexdigest(),
    }

    # Sign the request with SigV4.
    request = AWSRequest(method="POST", url=url, data=body, headers=headers)
    SigV4Auth(credentials, "s3", region).add_auth(request)

    prepared = request.prepare()
    response = _bundle_pool.urlopen(
        prepared.method,
        prepared.url,
        body=prepared.body,
        headers=dict(prepared.headers),
        preload_content=False,
    )

    if response.status >= HTTPStatus.BAD_REQUEST:
        try:
            error_body = response.read().decode("utf-8", errors="replace")
        except Exception:
            error_body = ""
        finally:
            response.close()
        msg = f"Bundle request failed (HTTP {response.status}): {error_body}"
        raise Exception(msg)  # noqa: TRY002

    return BundleResponse(
        body=response,
        content_type=response.headers.get("Content-Type", "application/x-tar"),
        status_code=response.status,
        headers=dict(response.headers),
    )
