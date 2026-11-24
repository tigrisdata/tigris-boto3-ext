"""Examples for access key scope with tigris-boto3-ext.

A no_default_allow access key can do only what its bucket roles and IAM
policies grant; a standard key also gets the default allowed operations, such
as creating and listing buckets. Tigris takes the scope as a header on IAM
CreateAccessKey. The scope is set at creation and cannot be read back later.
See https://www.tigrisdata.com/docs/iam/manage-access-key/
"""

from typing import Any

import boto3

from tigris_boto3_ext import (
    TigrisAccessKeyScope,
    create_access_key,
    with_access_key_scope,
)

iam = boto3.client(
    "iam",
    endpoint_url="https://iam.storage.dev",
    region_name="auto",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)

created = []


def example_helper():
    """Create a key that can only do what its roles and policies allow."""
    print("\n=== create_access_key helper ===")

    key = create_access_key(iam, "ci-deploy")
    created.append(key["AccessKey"]["AccessKeyId"])
    print(
        f"Created ...{key['AccessKey']['AccessKeyId'][-4:]} with scope {key['Scope']}"
    )


def example_context_manager():
    """Scope every key created inside the block."""
    print("\n=== TigrisAccessKeyScope context manager ===")

    with TigrisAccessKeyScope(iam, "no_default_allow"):
        for name in ("ci-deploy-1", "ci-deploy-2"):
            response = iam.create_access_key(UserName=name)
            created.append(response["AccessKey"]["AccessKeyId"])
            print(f"Created {name} with no default allowed operations")


def example_decorator():
    """Wrap an existing key-creation function without changing its body."""
    print("\n=== @with_access_key_scope decorator ===")

    @with_access_key_scope("no_default_allow")
    def create_key(client: Any, name: str) -> dict[str, Any]:
        return client.create_access_key(UserName=name)

    response = create_key(iam, "ci-deploy-3")
    created.append(response["AccessKey"]["AccessKeyId"])
    print("Created ci-deploy-3 with no default allowed operations")


def example_validation():
    """Unknown scopes are rejected before any request is sent."""
    print("\n=== Validation ===")

    for scope in ("NO_DEFAULT_ALLOW", "read-only"):
        try:
            create_access_key(iam, "never-created", scope)
        except ValueError as e:
            print(f"Rejected {scope!r}: {e}")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Access Key Scope Usage Examples")
    print("=" * 50)

    try:
        example_helper()
        example_context_manager()
        example_decorator()
        example_validation()
    finally:
        failed = []
        for key_id in created:
            try:
                iam.delete_access_key(AccessKeyId=key_id)
            except Exception:
                failed.append(key_id)
        if failed:
            print(f"Could not delete {len(failed)} keys; delete them in the dashboard")
