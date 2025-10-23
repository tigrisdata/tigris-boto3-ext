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


@pytest.fixture
def cleanup_buckets(s3_client, test_bucket_prefix):
    """Clean up test buckets after tests."""
    created_buckets = []

    yield created_buckets

    # Cleanup: delete all test buckets
    failures = []
    for bucket_name in created_buckets:
            # Delete all objects in the bucket first
            try:
                response = s3_client.list_objects_v2(Bucket=bucket_name)
                if "Contents" in response:
                    for obj in response["Contents"]:
                        s3_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
            except Exception:
                pass

            # Delete the bucket
            e = delete_bucket(s3_client, bucket_name)
            if e is not None:
                 failures.append(f"{bucket_name}: {e}")

    if failures:
        pytest.fail(f"Cleanup failed for {len(failures)} bucket(s):\n" + "\n".join(failures))


def delete_bucket(s3_client, bucket_name, retries=3, delay=1):
    last_exception = None
    for attempt in range(retries):
        try:
            s3_client.delete_bucket(Bucket=bucket_name)
            return None
        except Exception as e:
            last_exception = e
            if attempt < retries - 1: # Not sleeping after last attempt
                time.sleep(delay*(attempt + 1))
    return last_exception
