# CORS

Tigris implements the standard S3 CORS operations, so boto3's `put_bucket_cors`, `get_bucket_cors` and `delete_bucket_cors` work as they do on S3 and need nothing from `tigris_boto3_ext`. See the [Tigris CORS docs](https://www.tigrisdata.com/docs/buckets/cors/) for the rules and how they are evaluated.

What Tigris adds to the S3 contract:

- Every rule needs `MaxAgeSeconds` greater than zero. A configuration with one rule that leaves it out is rejected as a whole (`InvalidArgument`, "Max-age must be greater than zero").
- Unknown HTTP methods are rejected (`InvalidArgument`, "Invalid http method").

[`get_bucket_metadata`](bucket-info.md) carries the same rules under `raw["cors"]["rules"]` (`allowedOrigin`, `allowedMethods`, `allowedHeaders`, `exposeHeaders`, `maxAge`); the key is absent when the bucket has no rules.
