"""Integration tests for Bundle API functionality."""

import io
import json
import tarfile

from tigris_boto3_ext import (
    BUNDLE_COMPRESSION_GZIP,
    BUNDLE_COMPRESSION_ZSTD,
    BUNDLE_ON_ERROR_FAIL,
    BUNDLE_ON_ERROR_SKIP,
    BundleError,
    bundle_objects,
)

from .conftest import generate_bucket_name


class TestBundleBasic:
    """Test basic bundle fetch operations."""

    def test_bundle_single_object(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test bundling a single object."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "bundle-single-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="hello.txt", Body=b"Hello, world!")

        response = bundle_objects(s3_client, bucket_name, ["hello.txt"])

        with tarfile.open(fileobj=response, mode="r|") as tar:
            members = []
            for member in tar:
                if member.name == "__bundle_errors.json":
                    continue
                f = tar.extractfile(member)
                assert f is not None
                members.append((member.name, f.read()))

        assert len(members) == 1
        assert members[0] == ("hello.txt", b"Hello, world!")

    def test_bundle_multiple_objects(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test bundling multiple objects preserves ordering."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "bundle-multi-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)

        objects = {
            "dir/a.txt": b"aaa",
            "dir/b.txt": b"bbb",
            "dir/c.txt": b"ccc",
        }
        for key, body in objects.items():
            s3_client.put_object(Bucket=bucket_name, Key=key, Body=body)

        keys = list(objects.keys())
        response = bundle_objects(s3_client, bucket_name, keys)

        extracted = {}
        with tarfile.open(fileobj=response, mode="r|") as tar:
            for member in tar:
                if member.name == "__bundle_errors.json":
                    continue
                f = tar.extractfile(member)
                assert f is not None
                extracted[member.name] = f.read()

        assert extracted == objects

    def test_bundle_preserves_request_ordering(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that tar entries match the key ordering in the request."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "bundle-order-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)

        for i in range(5):
            s3_client.put_object(
                Bucket=bucket_name, Key=f"img_{i:03d}.jpg", Body=f"data_{i}".encode()
            )

        # Request in reverse order
        keys = [f"img_{i:03d}.jpg" for i in reversed(range(5))]
        response = bundle_objects(s3_client, bucket_name, keys)

        entry_names = []
        with tarfile.open(fileobj=response, mode="r|") as tar:
            for member in tar:
                if member.name == "__bundle_errors.json":
                    continue
                entry_names.append(member.name)

        assert entry_names == keys


class TestBundleCompression:
    """Test bundle compression options."""

    def test_bundle_gzip_compression(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test fetching a gzip-compressed bundle."""
        import gzip

        bucket_name = generate_bucket_name(test_bucket_prefix, "bundle-gzip-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"gzip test data")

        response = bundle_objects(
            s3_client, bucket_name, ["file.txt"], compression=BUNDLE_COMPRESSION_GZIP
        )

        with gzip.open(response, "rb") as gz:
            with tarfile.open(fileobj=gz, mode="r|") as tar:
                members = []
                for member in tar:
                    if member.name == "__bundle_errors.json":
                        continue
                    f = tar.extractfile(member)
                    assert f is not None
                    members.append((member.name, f.read()))

        assert len(members) == 1
        assert members[0] == ("file.txt", b"gzip test data")

    def test_bundle_zstd_compression(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test fetching a zstd-compressed bundle."""
        try:
            import zstandard
        except ImportError:
            import pytest

            pytest.skip("zstandard not installed")

        bucket_name = generate_bucket_name(test_bucket_prefix, "bundle-zstd-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="file.txt", Body=b"zstd test data")

        response = bundle_objects(
            s3_client, bucket_name, ["file.txt"], compression=BUNDLE_COMPRESSION_ZSTD
        )

        raw = response.read()
        dctx = zstandard.ZstdDecompressor()
        decompressed = dctx.decompress(raw)

        with tarfile.open(fileobj=io.BytesIO(decompressed), mode="r|") as tar:
            members = []
            for member in tar:
                if member.name == "__bundle_errors.json":
                    continue
                f = tar.extractfile(member)
                assert f is not None
                members.append((member.name, f.read()))

        assert len(members) == 1
        assert members[0] == ("file.txt", b"zstd test data")


class TestBundleErrorHandling:
    """Test bundle error modes."""

    def test_skip_mode_omits_missing_keys(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that skip mode silently omits missing objects."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "bundle-skip-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="exists.txt", Body=b"here")

        response = bundle_objects(
            s3_client,
            bucket_name,
            ["exists.txt", "missing.txt"],
            on_error=BUNDLE_ON_ERROR_SKIP,
        )

        extracted = {}
        errors = None
        with tarfile.open(fileobj=response, mode="r|") as tar:
            for member in tar:
                f = tar.extractfile(member)
                assert f is not None
                data = f.read()
                if member.name == "__bundle_errors.json":
                    errors = json.loads(data)
                else:
                    extracted[member.name] = data

        assert extracted == {"exists.txt": b"here"}
        # The error manifest should list the missing key
        assert errors is not None
        skipped_keys = [e["key"] for e in errors["skipped"]]
        assert "missing.txt" in skipped_keys

    def test_fail_mode_raises_on_missing_key(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that fail mode returns an error when a key is missing."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "bundle-fail-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="exists.txt", Body=b"here")

        import pytest

        with pytest.raises(BundleError) as exc_info:
            bundle_objects(
                s3_client,
                bucket_name,
                ["exists.txt", "no-such-key.txt"],
                on_error=BUNDLE_ON_ERROR_FAIL,
            )

        assert exc_info.value.status_code == 404


class TestBundleResponseMetadata:
    """Test BundleResponse metadata properties."""

    def test_response_has_object_count(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test that the response exposes object count."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "bundle-meta-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)
        for i in range(3):
            s3_client.put_object(
                Bucket=bucket_name, Key=f"obj_{i}.txt", Body=f"data_{i}".encode()
            )

        keys = [f"obj_{i}.txt" for i in range(3)]
        response = bundle_objects(s3_client, bucket_name, keys)

        # Drain the body so trailing headers are available
        with tarfile.open(fileobj=response, mode="r|") as tar:
            for _ in tar:
                pass

        if response.object_count is not None:
            assert response.object_count == 3

    def test_response_context_manager(
        self, s3_client, test_bucket_prefix, cleanup_buckets
    ):
        """Test using BundleResponse as a context manager."""
        bucket_name = generate_bucket_name(test_bucket_prefix, "bundle-ctx-")
        cleanup_buckets.append(bucket_name)

        s3_client.create_bucket(Bucket=bucket_name)
        s3_client.put_object(Bucket=bucket_name, Key="ctx.txt", Body=b"context test")

        with bundle_objects(s3_client, bucket_name, ["ctx.txt"]) as response:
            with tarfile.open(fileobj=response, mode="r|") as tar:
                member = next(
                    m for m in tar if m.name != "__bundle_errors.json"
                )
                f = tar.extractfile(member)
                assert f is not None
                assert f.read() == b"context test"
