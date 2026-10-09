# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
uv sync --all-extras

# Run all tests
uv run pytest tests/ -v

# Run a single test file or test
uv run pytest tests/test_object_bundle.py -v
uv run pytest tests/test_object_bundle.py::TestBundleResponse::test_read_delegates -v

# Run integration tests (requires AWS_ENDPOINT_URL_S3, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
uv run pytest tests/integration/ -v

# Lint and format
uv run ruff check tigris_boto3_ext
uv run ruff check --fix tigris_boto3_ext
uv run ruff format tigris_boto3_ext

# Type checking
uv run mypy tigris_boto3_ext

# Build
uv build
```

## Architecture

This library extends boto3's S3 client with Tigris-specific features (snapshots, forks, soft delete, bundle API) without modifying boto3 itself.

### Module layout

Modules are grouped by the resource their S3 operation targets, then by feature:

```
tigris_boto3_ext/
├── __init__.py          # flat public API: re-exports everything below
├── _internal.py         # HeaderInjector, registry, _guard_lock (not public)
├── buckets/
│   ├── info.py          # has_snapshot_enabled, get_bucket_info
│   ├── delete.py        # TigrisSoftDeleteEnabled, soft_delete_enabled, create_soft_delete_bucket, force_delete_bucket
│   ├── snapshots.py     # TigrisSnapshotEnabled, TigrisSnapshot, snapshot_enabled, with_snapshot, create_snapshot_bucket, create_snapshot, list_snapshots, delete_snapshot, get_snapshot_version
│   └── forks.py         # TigrisFork, forked_from, create_fork
└── objects/
    ├── rename.py        # TigrisRename, with_rename, rename_object
    ├── snapshots.py     # get_object_from_snapshot, list_objects_from_snapshot, head_object_from_snapshot
    ├── delete.py        # TigrisSoftDeleteView, with_soft_delete_view, list_deleted_objects, list_deleted_object_versions, purge_deleted_object, restore_deleted_object
    └── bundle.py        # BundleError, BundleResponse, bundle_objects
```

Placement rule: a function lives under the resource its S3 operation targets (`CreateBucket`/`DeleteBucket`/`HeadBucket` → `buckets/`, `GetObject`/`DeleteObject`/`ListObjectsV2` → `objects/`), in the module named for its feature. Each feature module holds its context manager class, decorator and helper functions together, under `""" Context Managers """`, `""" Decorators """` and `""" Helpers """` section markers. Everything public is re-exported from `__init__.py` and listed in `__all__`; users never import from a subpackage. Shared infrastructure stays in `_internal.py`.

### Feature footprint

Code is grouped by resource; docs and examples are grouped by feature, because users arrive with a task, not a resource (soft delete spans `buckets/delete.py` and `objects/delete.py` but has one docs page). A feature ships as a set, and a PR that adds or extends a feature touches every piece:

1. the module under `buckets/` or `objects/`, re-exported from `__init__.py` and listed in `__all__`
2. a unit test, `tests/test_<resource>_<feature>.py`
3. an integration test, `tests/integration/test_<feature>.py`
4. a docs page, `docs/<feature>.md`: what the feature is with a link to the Tigris docs, the helpers, context manager and decorator with short examples, a Headers table, caveats
5. a runnable example, `examples/<feature>_usage.py`, self-contained (creates what it uses)
6. one row in the README "Features" table

The README is the front door only: install, quick start, the feature table, the three usage patterns explained once, how it works, development. Feature details go in the feature's page, not the README.

### Header injection via boto3 events

Everything except the bundle API works by injecting custom `X-Tigris-*` headers into S3 requests through boto3's event system. `_internal.py` provides the `HeaderInjector` infrastructure. It registers handlers on `before-sign.s3.<Operation>` events so headers are included in the SigV4 signature. A global registry tracks `(client_id, event_name)` pairs to allow safe nesting of multiple context managers on the same client (inner injector wins on header conflicts). Helpers whose header changes the meaning of a destructive operation (`delete_snapshot`, `force_delete_bucket`, `restore_deleted_object`) take `_guard_lock` and refuse to run while another injector is active for the same operation on the same client.

### Bundle API (direct HTTP)

`objects/bundle.py` bypasses boto3 entirely. It uses urllib3 directly with manual SigV4 signing to POST to `/{bucket}?bundle`. This is because the bundle endpoint is a Tigris extension (not an S3 operation) that streams a tar archive response. `BundleResponse` wraps the streaming response as a file-like object compatible with `tarfile.open(mode="r|")`.

### Key Tigris headers

- `X-Tigris-Enable-Snapshot: true` — enable snapshots on bucket creation
- `X-Tigris-Snapshot: <bucket>` / `X-Tigris-Snapshot: true; name=<name>` — list/create snapshots
- `X-Tigris-Snapshot-Version: <version>` — read from a specific snapshot
- `X-Tigris-Fork-Source-Bucket` / `X-Tigris-Fork-Source-Bucket-Snapshot` — fork source
- `X-Tigris-Rename: true` — turns `CopyObject` into an in-place rename
- `X-Tigris-Soft-Delete: true` / `<days>` — enable soft delete on bucket creation (7–90 days)
- `X-Tigris-Soft-Delete: true` on `DeleteObject`, `ListObjectVersions`, `ListObjectsV2` — operate on soft-deleted versions (purge / list)
- `X-Tigris-Restore-Type: soft-delete` + `X-Tigris-Restore-Version` on `RestoreObject` — restore a soft-deleted object
- `X-Tigris-Force-Delete: true` on `DeleteBucket` — delete a non-empty bucket
- `X-Tigris-Bundle-Format`, `X-Tigris-Bundle-Compression`, `X-Tigris-Bundle-On-Error` — bundle request config

## Test structure

- **Unit tests** (`tests/test_*.py`): Mock boto3 clients and urllib3. Fast, no network.
- **Integration tests** (`tests/integration/`): Run against real Tigris. Skipped automatically when env vars are not set. Use `cleanup_buckets` fixture for automatic teardown. Bucket names are prefixed with `tigris-boto3-ext-test-` plus a UUID.

## Release process

1. Bump version in `pyproject.toml` and `tigris_boto3_ext/__init__.py`
2. Merge via PR (main is protected)
3. Tag: `git tag -a vX.Y.Z -m "Release vX.Y.Z"` and push tag
4. GitHub Actions builds, publishes to PyPI, and creates a GitHub release
