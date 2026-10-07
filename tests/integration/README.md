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
export $(cat .env | xargs)
```

## Running Integration Tests

### Run All Integration Tests

```bash
uv run pytest tests/integration/
```

### Run Specific Test File

```bash
# Test snapshots
uv run pytest tests/integration/test_snapshots.py

# Test forks
uv run pytest tests/integration/test_forks.py

# Test bucket info
uv run pytest tests/integration/test_bucket_info.py

# Test object rename
uv run pytest tests/integration/test_rename.py

# Test context managers
uv run pytest tests/integration/test_context_managers_integration.py

# Test decorators
uv run pytest tests/integration/test_decorators_integration.py

# Test Bundle API
uv run pytest tests/integration/test_bundle.py

# Test soft delete
uv run pytest tests/integration/test_soft_delete.py
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

- **`test_snapshots.py`**: Tests snapshot creation, listing, and data access
- **`test_forks.py`**: Tests bucket forking and data isolation
- **`test_bucket_info.py`**: Tests `has_snapshot_enabled` and `get_bucket_info` on snapshot-enabled, regular, forked, and fork-parent buckets
- **`test_rename.py`**: Tests in-place rename via the helper, context manager, and decorator, including nested and special-character keys
- **`test_snapshots.py`**: Tests snapshot creation, listing, deletion, and data access
- **`test_context_managers_integration.py`**: Tests context manager behavior
- **`test_decorators_integration.py`**: Tests decorator functionality
- **`test_bundle.py`**: Tests Bundle API streaming multi-object fetch
- **`test_soft_delete.py`**: Tests creating soft-delete buckets, the soft-delete view, purging a soft-deleted version, and restoring soft-deleted objects

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

The integration tests cover:

- ✅ Creating buckets with snapshot enabled
- ✅ Creating named snapshots
- ✅ Listing snapshots
- ✅ Accessing data from snapshots
- ✅ Creating forks from existing buckets
- ✅ Forking from specific snapshot versions
- ✅ Data isolation between forks and sources
- ✅ Context manager usage and nesting
- ✅ Decorator functionality
- ✅ Bucket info: snapshot-enabled flag and fork source metadata
- ✅ In-place object rename via helper, context manager, and decorator
- ✅ Deleting a snapshot without affecting other snapshots or the bucket
- ✅ Rejecting deletion of an unknown snapshot version
- ✅ Bundle API single and multi-object fetch
- ✅ Bundle compression (gzip, zstd)
- ✅ Bundle error handling (skip and fail modes)
- ✅ Bundle response metadata properties
- ✅ Complete workflows combining multiple features
- ✅ Creating buckets with soft delete enabled, with default and custom retention
- ✅ Listing soft-deleted objects through the soft-delete view
- ✅ Purging a soft-deleted version
- ✅ Restoring a soft-deleted object, by version and most recent
- ✅ Listing soft-deleted objects and versions, with live versions excluded on snapshot buckets.