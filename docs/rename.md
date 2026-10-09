# Object rename

Rename moves an object to a new key inside the same bucket without rewriting its data, like `mv` on a filesystem. Tigris implements it as a `CopyObject` request carrying the `X-Tigris-Rename: true` header: the copy becomes a rename, and the source key is gone when the call returns.

Tigris docs: [Object rename](https://www.tigrisdata.com/docs/objects/object-rename/).
Runnable example: [`examples/rename_usage.py`](../examples/rename_usage.py).

All names below are imported from `tigris_boto3_ext`.

## Rename an object

```python
from tigris_boto3_ext import TigrisRename, rename_object, with_rename

# Helper: one rename, header scoped to this call
rename_object(s3, "my-bucket", "old-name.txt", "new-name.txt")

# Context manager: several renames with plain copy_object calls
with TigrisRename(s3):
    for src, dst in [("a.txt", "renamed-a.txt"), ("b.txt", "renamed-b.txt")]:
        s3.copy_object(Bucket="my-bucket", CopySource={"Bucket": "my-bucket", "Key": src}, Key=dst)

# Decorator: the wrapped function must take the client as its first argument
@with_rename
def rename_in_dir(s3_client, bucket, directory, src, dst):
    return s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": f"{directory}/{src}"},
        Key=f"{directory}/{dst}",
    )
```

Extra keyword arguments to `rename_object` are passed through to `copy_object`.

## Notes

- **Keep the context tight.** While `TigrisRename` or `@with_rename` is active, every `copy_object` on that client is a rename, including ones issued from other threads. The helper is the safe default; reach for the context manager when you have a batch of renames and nothing else copying.
- **Pass `CopySource` as a dict.** The dict form lets botocore percent-encode the key. The string form `"bucket/key"` is treated as already encoded and corrupts keys containing spaces, `+`, `?` or `#`. `rename_object` does this for you.

## Headers

| Operation | Header | Meaning |
|---|---|---|
| `CopyObject` | `X-Tigris-Rename: true` | Move the object to the destination key instead of copying it |
