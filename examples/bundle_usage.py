"""Bundle API usage examples for tigris-boto3-ext.

The Bundle API fetches multiple objects in a single request as a streaming
tar archive — designed for ML training workloads that need to fetch
thousands of objects per batch without per-object HTTP overhead.
"""

import gzip
import json
import tarfile

import boto3

from tigris_boto3_ext import (
    BUNDLE_COMPRESSION_GZIP,
    BUNDLE_ON_ERROR_FAIL,
    BUNDLE_ON_ERROR_SKIP,
    BundleError,
    bundle_objects,
)

# Initialize boto3 S3 client
s3 = boto3.client(
    "s3",
    endpoint_url="https://t3.storage.dev",  # Tigris endpoint
    aws_access_key_id="your-access-key",
    aws_secret_access_key="your-secret-key",
)

BUCKET = "my-dataset-bucket"


def example_basic_bundle():
    """Example: Fetch multiple objects as a streaming tar archive."""
    print("\n=== Basic Bundle Fetch ===")

    keys = [
        "dataset/train/img_001.jpg",
        "dataset/train/img_002.jpg",
        "dataset/train/img_003.jpg",
    ]

    response = bundle_objects(s3, BUCKET, keys)

    with tarfile.open(fileobj=response, mode="r|") as tar:
        for member in tar:
            # Skip the error manifest entry
            if member.name == "__bundle_errors.json":
                continue
            f = tar.extractfile(member)
            if f is not None:
                data = f.read()
                print(f"  {member.name}: {len(data)} bytes")


def example_bundle_with_context_manager():
    """Example: Use BundleResponse as a context manager for automatic cleanup."""
    print("\n=== Bundle with Context Manager ===")

    keys = ["dataset/train/img_001.jpg", "dataset/train/img_002.jpg"]

    with bundle_objects(s3, BUCKET, keys) as response:
        with tarfile.open(fileobj=response, mode="r|") as tar:
            for member in tar:
                if member.name == "__bundle_errors.json":
                    continue
                f = tar.extractfile(member)
                if f is not None:
                    data = f.read()
                    print(f"  {member.name}: {len(data)} bytes")
    # response is automatically closed here


def example_bundle_with_gzip():
    """Example: Fetch a gzip-compressed bundle for bandwidth savings."""
    print("\n=== Gzip-Compressed Bundle ===")

    keys = ["dataset/metadata/labels.json", "dataset/metadata/config.yaml"]

    response = bundle_objects(
        s3, BUCKET, keys, compression=BUNDLE_COMPRESSION_GZIP
    )

    # Wrap with gzip decompressor before passing to tarfile
    with gzip.open(response, "rb") as gz:
        with tarfile.open(fileobj=gz, mode="r|") as tar:
            for member in tar:
                if member.name == "__bundle_errors.json":
                    continue
                f = tar.extractfile(member)
                if f is not None:
                    data = f.read()
                    print(f"  {member.name}: {len(data)} bytes")


def example_skip_mode_with_error_manifest():
    """Example: Handle missing objects gracefully with skip mode (default)."""
    print("\n=== Skip Mode — Tolerant of Missing Objects ===")

    keys = [
        "dataset/train/img_001.jpg",
        "dataset/train/does_not_exist.jpg",  # This key is missing
        "dataset/train/img_003.jpg",
    ]

    response = bundle_objects(s3, BUCKET, keys, on_error=BUNDLE_ON_ERROR_SKIP)

    found = []
    errors = None

    with tarfile.open(fileobj=response, mode="r|") as tar:
        for member in tar:
            f = tar.extractfile(member)
            if f is None:
                continue
            if member.name == "__bundle_errors.json":
                errors = json.loads(f.read())
            else:
                found.append(member.name)

    print(f"  Objects fetched: {found}")
    if errors:
        for entry in errors.get("skipped", []):
            print(f"  Skipped: {entry['key']} ({entry['reason']})")


def example_fail_mode():
    """Example: Use fail mode when every object must be present."""
    print("\n=== Fail Mode — Strict Validation ===")

    keys = [
        "dataset/inference/sample_001.jpg",
        "dataset/inference/missing.jpg",
    ]

    try:
        bundle_objects(s3, BUCKET, keys, on_error=BUNDLE_ON_ERROR_FAIL)
    except BundleError as e:
        print(f"  Bundle failed (HTTP {e.status_code}): {e}")
        print(f"  Server response: {e.body}")


def example_response_metadata():
    """Example: Inspect response metadata after fetching a bundle."""
    print("\n=== Response Metadata ===")

    keys = [f"dataset/train/img_{i:03d}.jpg" for i in range(10)]

    response = bundle_objects(s3, BUCKET, keys)

    # Drain the tar stream so trailing headers become available
    with tarfile.open(fileobj=response, mode="r|") as tar:
        count = sum(1 for m in tar if m.name != "__bundle_errors.json")

    print(f"  Entries in tar: {count}")
    if response.object_count is not None:
        print(f"  Server object count: {response.object_count}")
    if response.bundle_bytes is not None:
        print(f"  Server bundle bytes: {response.bundle_bytes}")
    if response.skipped_count is not None:
        print(f"  Server skipped count: {response.skipped_count}")


def example_ml_training_batch():
    """Example: Fetch a training batch for an ML pipeline.

    This simulates a DataLoader worker fetching a batch of images
    for training. In practice, the key list would come from a metadata
    index (parquet, CSV, database) and be shuffled each epoch.
    """
    print("\n=== ML Training Batch ===")

    # Simulate a shuffled batch of 32 image keys
    batch_keys = [f"dataset/train/img_{i:05d}.jpg" for i in range(32)]

    response = bundle_objects(s3, BUCKET, batch_keys)

    images = {}
    with tarfile.open(fileobj=response, mode="r|") as tar:
        for member in tar:
            if member.name == "__bundle_errors.json":
                continue
            f = tar.extractfile(member)
            if f is not None:
                images[member.name] = f.read()

    print(f"  Loaded {len(images)} images in a single HTTP request")
    # Feed images dict to training pipeline...


if __name__ == "__main__":
    print("Tigris boto3 Extensions - Bundle API Usage Examples")
    print("=" * 55)

    example_basic_bundle()
    example_bundle_with_context_manager()
    example_bundle_with_gzip()
    example_skip_mode_with_error_manifest()
    example_fail_mode()
    example_response_metadata()
    example_ml_training_batch()

    print("\n" + "=" * 55)
    print("Examples completed!")
