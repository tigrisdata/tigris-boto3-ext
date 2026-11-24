"""Integration tests for access key scope.

They need AWS_ENDPOINT_URL_IAM and a key that may create access keys, and delete every key
they create.
"""

import os
import time

import boto3
import pytest
from botocore.exceptions import ClientError

from tigris_boto3_ext import (
    TigrisAccessKeyScope,
    create_access_key,
    with_access_key_scope,
)

from .conftest import generate_bucket_name


@pytest.fixture
def iam_client(tigris_endpoint, aws_credentials):
    endpoint = os.environ.get("AWS_ENDPOINT_URL_IAM")
    if not endpoint:
        pytest.skip("AWS_ENDPOINT_URL_IAM not set")
    return boto3.client(
        "iam", endpoint_url=endpoint, region_name="auto", **aws_credentials
    )


@pytest.fixture
def created_keys(iam_client):
    keys = []
    yield keys
    for key_id in keys:
        try:
            iam_client.delete_access_key(AccessKeyId=key_id)
        except Exception:
            print(f"WARNING: could not delete test access key ...{key_id[-4:]}")  # noqa: T201


def _list_buckets_code(endpoint, key):
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=key["AccessKeyId"],
        aws_secret_access_key=key["SecretAccessKey"],
    )
    for _ in range(10):
        try:
            s3.list_buckets()
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
                time.sleep(3)
                continue
            return code
        return "allowed"
    return "key never became valid"


class TestAccessKeyScope:
    def test_no_default_allow_key_gets_no_default_operations(
        self, iam_client, created_keys, tigris_endpoint, test_bucket_prefix
    ):
        """A no_default_allow key cannot list buckets; a standard key can."""
        name = generate_bucket_name(test_bucket_prefix, "key-")

        scoped = create_access_key(iam_client, f"{name}-scoped")
        created_keys.append(scoped["AccessKey"]["AccessKeyId"])
        standard = create_access_key(iam_client, f"{name}-standard", "standard")
        created_keys.append(standard["AccessKey"]["AccessKeyId"])

        assert scoped["Scope"] == "no_default_allow"
        assert standard["Scope"] == "standard"
        assert (
            _list_buckets_code(tigris_endpoint, scoped["AccessKey"]) == "AccessDenied"
        )
        assert _list_buckets_code(tigris_endpoint, standard["AccessKey"]) == "allowed"

    def test_context_manager_and_decorator(
        self, iam_client, created_keys, test_bucket_prefix
    ):
        """Plain create_access_key calls inside the block or function get the scope."""
        name = generate_bucket_name(test_bucket_prefix, "key-")

        with TigrisAccessKeyScope(iam_client, "no_default_allow"):
            response = iam_client.create_access_key(UserName=f"{name}-ctx")
        created_keys.append(response["AccessKey"]["AccessKeyId"])
        assert (
            response["ResponseMetadata"]["HTTPHeaders"]["x-tigris-access-key-scope"]
            == "no_default_allow"
        )

        @with_access_key_scope("no_default_allow")
        def make_key(client, user_name):
            return client.create_access_key(UserName=user_name)

        response = make_key(iam_client, f"{name}-deco")
        created_keys.append(response["AccessKey"]["AccessKeyId"])
        assert (
            response["ResponseMetadata"]["HTTPHeaders"]["x-tigris-access-key-scope"]
            == "no_default_allow"
        )

    def test_without_the_header_keys_are_standard(
        self, iam_client, created_keys, test_bucket_prefix
    ):
        """A plain create_access_key outside any scope block creates a standard key."""
        name = generate_bucket_name(test_bucket_prefix, "key-")

        response = iam_client.create_access_key(UserName=f"{name}-plain")
        created_keys.append(response["AccessKey"]["AccessKeyId"])

        assert (
            response["ResponseMetadata"]["HTTPHeaders"]["x-tigris-access-key-scope"]
            == "standard"
        )
