"""Shared fixtures for integration tests."""

import os
import time
import uuid

import boto3
import pytest


@pytest.fixture
def tigris_endpoint():
    """Get Tigris S3 endpoint from environment."""
    # Check both AWS_ENDPOINT_URL_S3 (preferred) and AWS_ENDPOINT_URL
    endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL")
    if not endpoint:
        pytest.skip("AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL not set")
    return endpoint


@pytest.fixture
def aws_credentials():
    """Get AWS credentials from environment."""
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        pytest.skip("AWS credentials not set")

    return {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
    }


@pytest.fixture
def s3_client(tigris_endpoint, aws_credentials):
    """Create a real S3 client for Tigris."""
    return boto3.client(
        "s3",
        endpoint_url=tigris_endpoint,
        **aws_credentials,
    )


@pytest.fixture
def test_bucket_prefix():
    """Prefix for test buckets to avoid conflicts."""
    return "tigris-boto3-ext-test-"


def generate_bucket_name(prefix: str = "tigris-boto3-ext-test-", suffix: str = "") -> str:
    """
    Generate a unique bucket name for integration tests.

    Args:
        prefix: Bucket name prefix
        suffix: Optional suffix to add before the UUID (e.g., 'snapshot-', 'fork-')

    Returns:
        Unique bucket name with format: {prefix}{suffix}{uuid}
    """
    unique_id = uuid.uuid4().hex[:12]
    return f"{prefix}{suffix}{unique_id}"


def bucket_exists(s3_client, bucket_name):  # noqa: ANN001, ANN201
    """Check if a bucket exists using head_bucket (doesn't depend on list_buckets pagination)."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except Exception:
        return False


def _empty_bucket(s3_client, bucket_name):  # noqa: ANN001, ANN202
    """Delete all objects (including all versions and delete markers) from a bucket."""
    try:
        # Try versioned listing first (handles versioned buckets and delete markers).
        paginator = s3_client.get_paginator("list_object_versions")
        for page in paginator.paginate(Bucket=bucket_name):
            objects_to_delete = []
            for version in page.get("Versions", []):
                objects_to_delete.append(
                    {"Key": version["Key"], "VersionId": version["VersionId"]}
                )
            for marker in page.get("DeleteMarkers", []):
                objects_to_delete.append(
                    {"Key": marker["Key"], "VersionId": marker["VersionId"]}
                )
            if objects_to_delete:
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={"Objects": objects_to_delete},
                )
    except Exception:
        # Fall back to simple listing for non-versioned buckets.
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get("Contents", []):
                    s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
        except Exception:
            pass


@pytest.fixture
def cleanup_buckets(s3_client, test_bucket_prefix):
    """Clean up test buckets after tests."""
    created_buckets = []

    yield created_buckets

    # Empty all buckets first, then delete in multiple passes to handle
    # fork dependencies (fork must be deleted before source).
    for bucket_name in created_buckets:
        _empty_bucket(s3_client, bucket_name)

    # Multiple passes: fork dependencies may require deleting forks before sources.
    # Reversed order handles the common case (forks registered after sources).
    remaining = list(reversed(created_buckets))
    for _pass in range(3):
        if not remaining:
            break
        still_remaining = []
        for bucket_name in remaining:
            _empty_bucket(s3_client, bucket_name)
            e = delete_bucket(s3_client, bucket_name)
            if e is not None:
                still_remaining.append(bucket_name)
        remaining = still_remaining
        if remaining:
            time.sleep(2)

    if remaining:
        # Best-effort: don't fail the test for cleanup issues.
        for name in remaining:
            print(f"WARNING: could not delete test bucket: {name}")  # noqa: T201


def delete_bucket(s3_client, bucket_name, retries=3, delay=1):  # noqa: ANN001, ANN002, ANN003, ANN201
    last_exception = None
    for attempt in range(retries):
        try:
            s3_client.delete_bucket(Bucket=bucket_name)
            return None
        except Exception as e:
            last_exception = e
            if attempt < retries - 1:  # Not sleeping after last attempt
                time.sleep(delay * (attempt + 1))
    return last_exception
