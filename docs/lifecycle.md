# Lifecycle rules

Tigris implements the standard S3 lifecycle operations, so boto3's `put_bucket_lifecycle_configuration`, `get_bucket_lifecycle_configuration` and `delete_bucket_lifecycle` work as they do on S3 and need nothing from `tigris_boto3_ext`. A time-to-live is an expiration rule without a prefix. See the Tigris docs for [lifecycle rules](https://www.tigrisdata.com/docs/buckets/object-lifecycle-rules/) and [object expiration](https://www.tigrisdata.com/docs/buckets/objects-expiration/).

What Tigris adds to the S3 contract:

- Transitions go to `STANDARD_IA`, `GLACIER` or `GLACIER_IR` only; an expiration needs positive `Days` or a `Date` (`InvalidArgument` otherwise).
- At most 10 rules per bucket.
- Lifecycle rules are refused on [snapshot buckets](snapshots.md) and on any bucket that was ever snapshot-enabled (`InvalidArgument`). The reverse holds too: a bucket with rules cannot become a snapshot bucket ([`update_bucket(snapshot_enabled=True)`](settings.md)).
- Objects in `GLACIER` need a restore before they can be read; `GLACIER_IR` does not.

[`get_bucket_metadata`](bucket-info.md) carries the same rules under `raw["lifecycle_rules"]` (`id`, `status` `1` enabled / `2` disabled, `transitions`, `expiration`, `filter.prefix`; dates as `YYYY-MM-DD`); the key is absent when the bucket has no rules.
