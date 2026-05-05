"""Examples for renaming objects with tigris-boto3-ext.

Tigris implements object rename as a CopyObject request with the
``X-Tigris-Rename: true`` header — no data is rewritten, only the key
is updated. See https://www.tigrisdata.com/docs/objects/object-rename/
"""

import boto3

from tigris_boto3_ext import TigrisRename, rename_object, with_rename

s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)


def example_helper():
    """Easiest path: scoped to a single rename call."""
    print("\n=== rename_object helper ===")
    rename_object(s3, "my-bucket", "old-name.txt", "new-name.txt")
    print("Renamed old-name.txt -> new-name.txt")


def example_context_manager():
    """Use the context manager when you want to issue several renames."""
    print("\n=== TigrisRename context manager ===")
    bucket = "my-bucket"
    pairs = [
        ("a.txt", "renamed-a.txt"),
        ("b.txt", "renamed-b.txt"),
        ("c.txt", "renamed-c.txt"),
    ]

    with TigrisRename(s3):
        for src, dst in pairs:
            s3.copy_object(
                Bucket=bucket,
                CopySource=f"{bucket}/{src}",
                Key=dst,
            )
            print(f"Renamed {src} -> {dst}")


def example_decorator():
    """Wrap a function so its CopyObject calls become renames."""
    print("\n=== @with_rename decorator ===")

    @with_rename
    def rename_in_dir(client, bucket, directory, src, dst):
        return client.copy_object(
            Bucket=bucket,
            CopySource=f"{bucket}/{directory}/{src}",
            Key=f"{directory}/{dst}",
        )

    rename_in_dir(s3, "my-bucket", "logs", "old.log", "new.log")
    print("Renamed logs/old.log -> logs/new.log")


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Rename Usage Examples")
    print("=" * 50)

    example_helper()
    example_context_manager()
    example_decorator()
