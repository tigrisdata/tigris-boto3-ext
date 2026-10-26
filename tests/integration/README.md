# Integration Tests

This directory contains integration tests for `tigris-boto3-ext` that test against a real Tigris S3 service.

## Prerequisites

1. **Tigris Account**: You need access to a Tigris S3-compatible service
2. **Credentials**: Valid access key ID and secret access key for Tigris
3. **Endpoint URL**: The Tigris S3 endpoint URL

## Setup

### Environment Variables

Set the following environment variables:

```bash
export AWS_ENDPOINT_URL_S3="https://t3.storage.dev"
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
```

Alternatively, you can use `AWS_ENDPOINT_URL` instead of `AWS_ENDPOINT_URL_S3`:

```bash
export AWS_ENDPOINT_URL="https://t3.storage.dev"
```

### Using a `.env` File

Create a `.env` file in the project root:

```text
AWS_ENDPOINT_URL_S3=https://t3.storage.dev
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY=your-secret-access-key
```

Then load it before running tests:

```bash
set -a; source .env; set +a
```

(`set -a` exports every variable the sourced file assigns. This form copes with values containing spaces or quotes, which `export $(cat .env | xargs)` does not.) The file is gitignored.

## Running Integration Tests

### Run All Integration Tests

```bash
uv run pytest tests/integration/
```

### Run Specific Test File

One file per feature, matching the pages under `docs/`:

```bash
uv run pytest tests/integration/test_create_bucket.py  # create bucket options
uv run pytest tests/integration/test_settings.py       # bucket settings
uv run pytest tests/integration/test_notifications.py  # object notifications
uv run pytest tests/integration/test_migration.py      # data migration
uv run pytest tests/integration/test_restore.py        # archive restore
uv run pytest tests/integration/test_presign.py        # presigned URLs
uv run pytest tests/integration/test_snapshots.py      # snapshots, including deletion
uv run pytest tests/integration/test_forks.py          # forks
uv run pytest tests/integration/test_bucket_info.py    # bucket info
uv run pytest tests/integration/test_listing.py        # bucket listing, fork listing
uv run pytest tests/integration/test_stats.py          # account stats
uv run pytest tests/integration/test_rename.py         # object rename
uv run pytest tests/integration/test_soft_delete.py    # soft delete and force delete
uv run pytest tests/integration/test_bundle.py         # Bundle API

# Cross-feature: context manager nesting and decorator combinations
uv run pytest tests/integration/test_context_managers_integration.py
uv run pytest tests/integration/test_decorators_integration.py
```

### Run Specific Test Class or Function

```bash
# Run a specific test class
uv run pytest tests/integration/test_snapshots.py::TestSnapshotCreation

# Run a specific test function
uv run pytest tests/integration/test_snapshots.py::TestSnapshotCreation::test_create_snapshot_with_helper
```

### Run with Verbose Output

```bash
uv run pytest tests/integration/ -v
```

### Run with Debug Output

```bash
uv run pytest tests/integration/ -vv -s
```

## Test Structure

- **`conftest.py`**: Shared fixtures for all integration tests
  - `tigris_endpoint`: Gets Tigris endpoint from environment
  - `aws_credentials`: Gets AWS credentials from environment
  - `s3_client`: Creates a real boto3 S3 client
  - `test_bucket_prefix`: Prefix for test bucket names
  - `cleanup_buckets`: Automatically cleans up test buckets after tests
  - `anonymous_status()`: Status of an unsigned GET to a bucket's public URL (`<bucket>.t3.tigrisfiles.io`; override the domain with `TIGRIS_PUBLIC_DOMAIN`)

- **`test_create_bucket.py`**: Tests creation options: default storage class, regions, public access and anonymous listing, via helper, context manager and decorator
- **`test_settings.py`**: Tests `update_bucket`: storage class, regions and cache control read back from the metadata endpoint, object ACL and nosniff toggles, public access via the bucket ACL and listing visibility, snapshot type round trip and refusals, rejected custom domain, missing bucket
- **`test_snapshots.py`**: Tests snapshot creation, listing, deletion, and data access
- **`test_forks.py`**: Tests bucket forking and data isolation
- **`test_bucket_info.py`**: Tests `has_snapshot_enabled` and `get_bucket_info` on snapshot-enabled, regular, forked, and fork-parent buckets
- **`test_listing.py`**: Tests `list_buckets` with every detail flag, filters and paging, and `list_forks` lineage and errors
- **`test_stats.py`**: Tests `get_stats` against a single-bucket page
- **`test_rename.py`**: Tests in-place rename via the helper, context manager, and decorator, including nested and special-character keys
- **`test_notifications.py`**: Tests `set_bucket_notifications`: configure with token auth and a filter, pause with basic auth, remove, all read back through `get_bucket_metadata`; rejected filter and missing bucket
- **`test_migration.py`**: Tests `set_bucket_migration` refusals (Tigris source, rejected credentials) and removal, and the object helpers on a bucket without a source
- **`test_restore.py`**: Tests `get_restore_info` and `restore_object` on GLACIER, STANDARD and GLACIER_IR objects: archived, in-progress, repeated restore, refusals, missing key
- **`test_presign.py`**: Tests `presign` download and upload URLs on a private bucket and a snapshot-pinned download
- **`test_soft_delete.py`**: Tests creating soft-delete buckets, the soft-delete view, listing, purging and restoring soft-deleted objects, and force-deleting non-empty buckets
- **`test_bundle.py`**: Tests Bundle API streaming multi-object fetch
- **`test_context_managers_integration.py`**: Tests context manager behavior across features, including nesting
- **`test_decorators_integration.py`**: Tests decorator functionality across features, including combinations

Unit tests live one level up in `tests/test_<resource>_<feature>.py` (for example `tests/test_bucket_snapshots.py`, `tests/test_object_delete.py`), mirroring the package layout; they mock the client and need no credentials.

## Test Bucket Naming

All test buckets are named `tigris-boto3-ext-test-<suffix><id>`, where `<suffix>` describes the test (for example `delete-snap-`) and `<id>` is a random 12-character hex string from `uuid4`, so names never collide. The `cleanup_buckets` fixture automatically removes these buckets after each test.

## Skipping Tests

If environment variables are not set, tests will be automatically skipped with a message:

```text
SKIPPED [1] tests/integration/conftest.py:10: AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL not set
SKIPPED [1] tests/integration/conftest.py:19: AWS credentials not set
```

## Troubleshooting

### Tests are Skipped

Ensure environment variables are set:

```bash
echo $AWS_ENDPOINT_URL_S3
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
```

### Bucket Already Exists Errors

The tests adds a random suffix so conflicts are not expected. Buckets left behind by an interrupted run can be removed with the commands under "Cleanup Failures".

### Cleanup Failures

If tests fail and leave buckets behind, you can manually clean them up:

```bash
# List test buckets
aws s3 ls --endpoint-url $AWS_ENDPOINT_URL_S3 | grep tigris-boto3-ext-test

# Remove a specific bucket, using the full name printed by the listing above
aws s3 rb s3://tigris-boto3-ext-test-<suffix><id> --endpoint-url $AWS_ENDPOINT_URL_S3 --force
```

### Soft-Delete Buckets After a Run

Buckets created by `test_soft_delete.py` have soft delete enabled. The tests purge their soft-deleted objects during cleanup, but deleting such a bucket only moves it to a recoverable state, so it stays in the deleted list until its retention window ends, at most eight days. Names carry a random id, so these leftovers never collide with later runs.

### Connection Errors

Verify your endpoint URL and credentials:

```bash
# Test connection
aws s3 ls --endpoint-url $AWS_ENDPOINT_URL_S3
```

## CI/CD Integration

### GitHub Actions

Add secrets to your repository and use them in your workflow:

```yaml
- name: Run integration tests
  env:
    AWS_ENDPOINT_URL_S3: ${{ secrets.AWS_ENDPOINT_URL_S3 }}
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
  run: |
    uv run pytest tests/integration/ -v
```

## Notes

- **Real Resources**: These tests create and delete real S3 buckets in Tigris
- **Costs**: Be aware of any costs associated with bucket operations
- **Rate Limits**: Tigris may have rate limits; tests use timestamps to avoid conflicts
- **Cleanup**: Tests automatically clean up resources, but manual cleanup may be needed if tests are interrupted
- **Snapshot Versions**: Some tests note that snapshot versions would come from Tigris responses in real usage

## Test Coverage

The integration tests cover, by feature:

**Create bucket**
- ✅ Default storage class, single/dual/multi-region placement, public-read access and anonymous listing, with gateway defaults checked

**Snapshots**
- ✅ Creating buckets with snapshot enabled
- ✅ Creating named snapshots
- ✅ Listing snapshots
- ✅ Accessing data from snapshots
- ✅ Deleting a snapshot without affecting other snapshots or the bucket
- ✅ Rejecting deletion of an unknown snapshot version

**Forks**
- ✅ Creating forks from existing buckets
- ✅ Forking from specific snapshot versions
- ✅ Data isolation between forks and sources

**Bucket listing**
- ✅ Type, visibility, regions, fork lineage and owner per entry; unrequested fields None; only-forks filter; paging
- ✅ Forks of a parent with lineage, empty list for a non-parent, AccessDenied for an unknown source

**Storage stats**
- ✅ Account stats summed across pages, bounded by a single-bucket page

**Bucket info**
- ✅ Snapshot-enabled flag and fork source metadata
- ✅ Metadata endpoint: defaults, fork parent and child lineage, soft delete, creation options, size estimates, `TigrisAPIError` on a missing bucket

**Object rename**
- ✅ In-place object rename via helper, context manager, and decorator

**Soft delete**
- ✅ Creating buckets with soft delete enabled, with default and custom retention
- ✅ Listing soft-deleted objects and versions through the soft-delete view, with live versions excluded on snapshot buckets
- ✅ Purging a soft-deleted version
- ✅ Restoring a soft-deleted object, by version and most recent
- ✅ Listing soft-deleted buckets with pagination, restoring one with its objects, and the rejections for a live or unknown bucket
- ✅ Enabling, re-windowing and disabling soft delete on an existing bucket, with deletes becoming permanent once disabled

**Force delete**
- ✅ Force-deleting non-empty buckets, with and without soft delete

**Bundle API**
- ✅ Single and multi-object fetch
- ✅ Compression (gzip, zstd)
- ✅ Error handling (skip and fail modes)
- ✅ Response metadata properties

**Across features**
- ✅ Context manager usage and nesting
- ✅ Decorator functionality and combinations
- ✅ Complete workflows combining multiple features