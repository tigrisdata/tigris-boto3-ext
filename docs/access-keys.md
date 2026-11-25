# Access key scope

Every Tigris access key has a scope. A `standard` key, the default, gets the [default allowed operations](https://www.tigrisdata.com/docs/iam/manage-access-key/#default-allowed-operations) on top of its bucket roles and IAM policies: it can create and list buckets, create access keys, and manage the keys and policies it created. A `no_default_allow` key can do only what its bucket roles and policies grant. Use `no_default_allow` for keys you hand to users or CI jobs, so they cannot create buckets they then own.

Over the IAM API, Tigris takes the scope as the `X-Tigris-Access-Key-Scope` header on `CreateAccessKey`. This feature sets that header on a boto3 IAM client.

Tigris docs: [Manage access keys](https://www.tigrisdata.com/docs/iam/manage-access-key/).
Runnable example: [`examples/access_keys_usage.py`](../examples/access_keys_usage.py).

All names below are imported from `tigris_boto3_ext`. They take an IAM client, not the S3 one:

```python
import boto3

iam = boto3.client("iam", endpoint_url="https://iam.storage.dev", region_name="auto")
```

## Create a key with a scope

```python
from tigris_boto3_ext import TigrisAccessKeyScope, create_access_key, with_access_key_scope

# Helper: one call; Scope is the scope Tigris applied
key = create_access_key(iam, "ci-deploy")                     # no_default_allow
key = create_access_key(iam, "admin-tooling", "standard")
print(key["AccessKey"]["AccessKeyId"], key["Scope"])

# Context manager: every create_access_key inside gets the scope
with TigrisAccessKeyScope(iam, "no_default_allow"):
    iam.create_access_key(UserName="ci-deploy-1")
    iam.create_access_key(UserName="ci-deploy-2")

# Decorator (called): the wrapped function takes the IAM client first
@with_access_key_scope("no_default_allow")
def create_ci_key(iam_client, name):
    return iam_client.create_access_key(UserName=name)
```

The helper defaults to `no_default_allow`; a plain `create_access_key` without this feature creates a `standard` key. The scope is `standard` or `no_default_allow`, written exactly so; anything else raises `ValueError` before any request is sent.

## What to know

- **The scope is set at creation.** `update_access_key` does not change it, and `list_access_keys` does not report it. To change the scope of an existing key, use the Tigris Dashboard, `tigris access-keys set <id> --default-permissions enable|disable`, or `setAccessKeyScope` in [`@tigrisdata/iam`](https://www.npmjs.com/package/@tigrisdata/iam).
- **Read the applied scope from the response.** Tigris answers `CreateAccessKey` with the key's scope in the `x-tigris-access-key-scope` response header; `create_access_key` returns it as `Scope`.
- **A `no_default_allow` key cannot create keys**, since creating access keys is one of the default allowed operations. Create scoped keys with a `standard` or admin key.

**Concurrency.** As with every feature here, the header is registered on the client for the duration of the call or block, so a `create_access_key` issued from another thread on the same IAM client in that window picks it up too. `create_access_key` refuses to run while another scope is active on the same client. Use a client per thread; see [Thread safety](../README.md#thread-safety).

## Headers

| Operation | Header | Meaning |
|---|---|---|
| `CreateAccessKey` | `X-Tigris-Access-Key-Scope: standard` / `no_default_allow` | Scope of the new key |
| `CreateAccessKey` response | `x-tigris-access-key-scope` | The scope the key got |
