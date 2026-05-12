"""Trial agent that runs inside one sprite (one fork bucket).

Reads scoped Tigris credentials, the fork bucket name, and a trial id from
env vars (set by the orchestrator via ``sprite exec --env``). Reads a seed
from the fork, does some trivial work, and writes a result. The agent has
no access to the orchestrator's credentials or any sibling fork — only
its own bucket-scoped Editor key.
"""

from __future__ import annotations

import os
import sys

import boto3

SEED_KEY = "input/seed.txt"
RESULT_KEY = "output/result.txt"


def main() -> int:
    bucket = os.environ["FORK_BUCKET"]
    trial_id = os.environ["TRIAL_ID"]

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["AWS_ENDPOINT_URL_S3"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "auto"),
    )

    seed = s3.get_object(Bucket=bucket, Key=SEED_KEY)["Body"].read().decode()
    output = f"trial-{trial_id} on sprite {os.uname().nodename}: processed {seed!r}"
    s3.put_object(Bucket=bucket, Key=RESULT_KEY, Body=output.encode())
    print(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
