"""Internal IAM helpers for provisioning bucket-scoped Tigris access keys.

Tigris IAM is AWS-IAM-compatible, so we use the standard boto3 IAM client
pointed at ``https://iam.storageapi.dev`` (override with the
``TIGRIS_IAM_ENDPOINT`` env var). The bucket-scoped key flow is:

    1. ``create_access_key()``  — gets a fresh access key under Tigris's
       implicit ``"auto"`` user. The response carries the user name we
       need for subsequent calls.
    2. ``create_policy(PolicyName, PolicyDocument)`` — a bucket-scoped
       policy that grants the requested role (Editor → s3:*, ReadOnly →
       GetObject/ListBucket).
    3. ``attach_user_policy(UserName, PolicyArn)`` — wires the policy to
       the access-key user.

Teardown reverses the sequence (detach → delete policy → delete access key).
"""

import json
import os
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional

import boto3

if TYPE_CHECKING:
    from mypy_boto3_iam.client import IAMClient
    from mypy_boto3_s3.client import S3Client
else:
    IAMClient = object
    S3Client = object


DEFAULT_IAM_ENDPOINT = "https://iam.storageapi.dev"


def _iam_endpoint() -> str:
    return os.environ.get("TIGRIS_IAM_ENDPOINT") or DEFAULT_IAM_ENDPOINT


def _build_iam_client(s3_client: S3Client) -> IAMClient:
    """Build a boto3 IAM client reusing the S3 client's credentials and region."""
    creds = s3_client._request_signer._credentials.get_frozen_credentials()  # type: ignore[attr-defined]  # noqa: SLF001
    return boto3.client(
        "iam",
        endpoint_url=_iam_endpoint(),
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_session_token=creds.token,
        region_name=s3_client.meta.region_name or "auto",
    )


def _policy_document(bucket: str, role: str) -> str:
    """Build a bucket-scoped IAM policy document for the given role."""
    if role == "Editor":
        actions: Any = "s3:*"
    elif role == "ReadOnly":
        actions = [
            "s3:GetObject",
            "s3:ListBucket",
            "s3:GetBucketLocation",
        ]
    else:
        msg = f"role must be 'Editor' or 'ReadOnly', got {role!r}"
        raise ValueError(msg)

    return json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": actions,
                    "Resource": [
                        f"arn:aws:s3:::{bucket}",
                        f"arn:aws:s3:::{bucket}/*",
                    ],
                },
            ],
        }
    )


def create_scoped_access_key(
    s3_client: S3Client,
    name: str,
    bucket: str,
    role: str,
) -> dict[str, str]:
    """Create a bucket-scoped Tigris access key.

    Args:
        s3_client: boto3 S3 client. Credentials and region are reused.
        name: Logical name. Used as the policy name (suffixed with
            ``-policy``) and any other naming hooks.
        bucket: Bucket the access key should be scoped to.
        role: ``"Editor"`` or ``"ReadOnly"``.

    Returns:
        Dict with ``access_key_id``, ``secret_access_key``, ``user_name``,
        and ``policy_arn`` — the last two are needed for teardown.

    Raises:
        ValueError: If ``role`` is not a recognized value.
        botocore.exceptions.ClientError: On IAM-side failures.
    """
    iam = _build_iam_client(s3_client)
    policy_doc = _policy_document(bucket, role)

    key_resp = iam.create_access_key()
    access_key = key_resp["AccessKey"]
    access_key_id = access_key["AccessKeyId"]
    secret = access_key["SecretAccessKey"]

    # Tigris IAM treats the access key id as the addressable "user" for
    # AttachUserPolicy. The UserName field in create_access_key's response
    # (typically "auto") is informational, not the handle to use here.
    policy_arn: Optional[str] = None
    try:
        policy_resp = iam.create_policy(
            PolicyName=f"{name}-policy",
            PolicyDocument=policy_doc,
        )
        policy_arn = policy_resp["Policy"]["Arn"]
        iam.attach_user_policy(UserName=access_key_id, PolicyArn=policy_arn)
    except Exception:
        # If policy creation/attach fails, roll back the bare access key
        # so we don't leak unscoped credentials.
        with suppress(Exception):
            iam.delete_access_key(AccessKeyId=access_key_id)
        if policy_arn is not None:
            with suppress(Exception):
                iam.delete_policy(PolicyArn=policy_arn)
        raise

    return {
        "access_key_id": access_key_id,
        "secret_access_key": secret,
        "user_name": access_key_id,
        "policy_arn": policy_arn,
    }


def delete_scoped_access_key(
    s3_client: S3Client,
    *,
    access_key_id: str,
    user_name: Optional[str] = None,
    policy_arn: Optional[str] = None,
) -> None:
    """Tear down a bucket-scoped Tigris access key.

    Best-effort: each step is independently suppressed so a single IAM
    failure can't strand the rest. Order matters — detach before deleting
    the policy and the key.
    """
    iam = _build_iam_client(s3_client)

    # Tigris IAM uses the access key id itself as the "user" handle for
    # AttachUserPolicy / DetachUserPolicy.
    detach_user = user_name or access_key_id

    if policy_arn:
        with suppress(Exception):
            iam.detach_user_policy(UserName=detach_user, PolicyArn=policy_arn)
        with suppress(Exception):
            iam.delete_policy(PolicyArn=policy_arn)
    with suppress(Exception):
        iam.delete_access_key(AccessKeyId=access_key_id)
