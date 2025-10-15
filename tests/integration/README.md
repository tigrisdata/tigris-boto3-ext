# Integration Tests

This directory contains integration tests for `tigris-boto3-ext` that test against a real Tigris S3 service.

## Prerequisites

1. **Tigris Account**: You need access to a Tigris S3-compatible service
2. **AWS Credentials**: Valid AWS access key ID and secret access key for Tigris
3. **Endpoint URL**: The Tigris S3 endpoint URL

## Setup

### Environment Variables

Set the following environment variables:

```bash
export AWS_ENDPOINT_URL_S3="https://fly.storage.tigris.dev"
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
```

Alternatively, you can use `AWS_ENDPOINT_URL` instead of `AWS_ENDPOINT_URL_S3`:

```bash
export AWS_ENDPOINT_URL="https://fly.storage.tigris.dev"
```

### Using a `.env` File

Create a `.env` file in the project root:

```
AWS_ENDPOINT_URL_S3=https://fly.storage.tigris.dev
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

# Test context managers
uv run pytest tests/integration/test_context_managers_integration.py

# Test decorators
uv run pytest tests/integration/test_decorators_integration.py

# Test TigrisS3Client
uv run pytest tests/integration/test_client_integration.py
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
- **`test_context_managers_integration.py`**: Tests context manager behavior
- **`test_decorators_integration.py`**: Tests decorator functionality
- **`test_client_integration.py`**: Tests TigrisS3Client wrapper

## Test Bucket Naming

All test buckets are prefixed with `tigris-boto3-ext-test-` followed by a timestamp to avoid conflicts. The `cleanup_buckets` fixture automatically removes these buckets after each test.

## Skipping Tests

If environment variables are not set, tests will be automatically skipped with a message:

```
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

The tests use timestamps to create unique bucket names. If you encounter bucket name conflicts, ensure your system clock is correct.

### Cleanup Failures

If tests fail and leave buckets behind, you can manually clean them up:

```bash
# List test buckets
aws s3 ls --endpoint-url $AWS_ENDPOINT_URL_S3 | grep tigris-boto3-ext-test

# Remove a specific bucket
aws s3 rb s3://tigris-boto3-ext-test-<timestamp> --endpoint-url $AWS_ENDPOINT_URL_S3 --force
```

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
- ✅ TigrisS3Client wrapper methods
- ✅ Complete workflows combining multiple features
