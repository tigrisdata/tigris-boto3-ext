"""End-to-end multi-agent workflow on top of agent_kit.

This is a small but realistic shape of how an agent runtime would use
the agent_kit primitives in production:

  1. **Orchestrator** spins up a workspace, seeds it with shared input
     data, takes a checkpoint, then forks ``N`` independent buckets —
     one per **trial agent** — each with its own least-privilege
     ``Editor`` access key.

  2. The trial agents run *concurrently* (here via a thread pool) and
     each only sees the credentials for its own fork. They read the
     seed, do some work, and write their results back into their fork.

  3. The orchestrator collects every trial's result by reading from the
     fork buckets, picks a winner, and tears everything down — buckets
     and scoped IAM keys both — using ``teardown_forks`` /
     ``teardown_workspace`` (force-delete via Tigris-Force-Delete).

  4. To demonstrate restore, the workflow then drifts the workspace
     bucket forward, calls ``restore(...)`` with the earlier checkpoint
     id, and asserts the restored fork holds the original state.

Run against real Tigris:

    export AWS_ENDPOINT_URL_S3=https://t3.storage.dev
    export AWS_ACCESS_KEY_ID=...
    export AWS_SECRET_ACCESS_KEY=...
    uv run python examples/multi_agent_workflow.py
"""

from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

import boto3

from tigris_boto3_ext import (
    Fork,
    ForkSet,
    Workspace,
    checkpoint,
    create_forks,
    create_workspace,
    delete_bucket,
    restore,
    teardown_forks,
    teardown_workspace,
)

NUM_TRIALS = 4
SEED_KEY = "input/seed.txt"
RESULT_KEY = "output/result.txt"


def _orchestrator_client() -> Any:
    """The orchestrator's S3 client — uses the operator's full-power creds."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_ENDPOINT_URL")
        or "https://t3.storage.dev",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "your-access-key"),
        aws_secret_access_key=os.environ.get(
            "AWS_SECRET_ACCESS_KEY", "your-secret-key"
        ),
        region_name=os.environ.get("AWS_REGION", "auto"),
    )


def _agent_client(fork: Fork) -> Any:
    """An S3 client that ONLY has access to one fork bucket.

    The trial agent never sees the orchestrator's keys; this is the whole
    point of provisioning per-fork credentials with ``credentials_role``.
    """
    if fork.credentials is None:
        msg = f"fork {fork.bucket!r} has no scoped credentials"
        raise RuntimeError(msg)
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_ENDPOINT_URL")
        or "https://t3.storage.dev",
        aws_access_key_id=fork.credentials.access_key_id,
        aws_secret_access_key=fork.credentials.secret_access_key,
        region_name=os.environ.get("AWS_REGION", "auto"),
    )


@dataclass
class TrialResult:
    trial_id: int
    fork_bucket: str
    output: str


def run_trial(trial_id: int, fork: Fork) -> TrialResult:
    """A trial agent: read the seed from its fork, do work, write a result.

    The agent uses ONLY its scoped credentials — it has no way to reach
    the base workspace or any sibling fork.
    """
    client = _agent_client(fork)
    seed = client.get_object(Bucket=fork.bucket, Key=SEED_KEY)["Body"].read().decode()
    output = f"trial-{trial_id}: processed {seed!r} (len={len(seed)})"
    client.put_object(Bucket=fork.bucket, Key=RESULT_KEY, Body=output.encode())
    return TrialResult(trial_id=trial_id, fork_bucket=fork.bucket, output=output)


def fan_out_trials(forks: ForkSet) -> list[TrialResult]:
    """Run all trial agents concurrently and collect their results."""
    results: list[TrialResult] = []
    with ThreadPoolExecutor(max_workers=len(forks.forks)) as pool:
        futures = {
            pool.submit(run_trial, i, fork): i for i, fork in enumerate(forks.forks)
        }
        for future in as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda r: r.trial_id)
    return results


def main() -> None:
    s3 = _orchestrator_client()
    run_id = uuid.uuid4().hex[:8]

    # Stage 1 — orchestrator sets up the workspace and seeds shared input.
    workspace_name = f"akwf-{run_id}"
    print(f"[orchestrator] create_workspace({workspace_name})")
    ws: Workspace = create_workspace(s3, workspace_name)
    s3.put_object(Bucket=ws.bucket, Key=SEED_KEY, Body=b"hello-from-orchestrator")

    forks: ForkSet | None = None
    try:
        # Stage 2 — capture a checkpoint of the seed state for later restore.
        ck = checkpoint(s3, ws.bucket, name=f"seed-{run_id}")
        print(f"[orchestrator] checkpoint snapshot_id={ck.snapshot_id}")

        # Stage 3 — fork N times, one Editor-scoped access key per fork.
        forks = create_forks(
            s3,
            ws.bucket,
            count=NUM_TRIALS,
            prefix=f"akwf-{run_id}-trial",
            credentials_role="Editor",
        )
        for fork in forks.forks:
            scoped = "yes" if fork.credentials else "no"
            print(f"[orchestrator]   fork={fork.bucket}  scoped-creds={scoped}")

        # Stage 4 — fan out the trial agents (each uses ONLY its own creds).
        print(f"[orchestrator] running {NUM_TRIALS} trial agents in parallel...")
        results = fan_out_trials(forks)

        # Stage 5 — orchestrator (with full creds) collects every fork's result.
        print("[orchestrator] collecting results:")
        for r in results:
            persisted = (
                s3.get_object(Bucket=r.fork_bucket, Key=RESULT_KEY)["Body"]
                .read()
                .decode()
            )
            print(f"[orchestrator]   {r.trial_id}: {persisted}")
        winner = max(results, key=lambda r: len(r.output))
        print(f"[orchestrator] winner: trial-{winner.trial_id}")

        # Stage 6 — drift the workspace and demonstrate restore.
        s3.put_object(Bucket=ws.bucket, Key=SEED_KEY, Body=b"polluted")
        restored = restore(s3, ws.bucket, ck.snapshot_id)
        try:
            seed_after = (
                s3.get_object(Bucket=restored, Key=SEED_KEY)["Body"].read().decode()
            )
            print(f"[orchestrator] restored fork {restored!r} seed={seed_after!r}")
            if seed_after != "hello-from-orchestrator":
                msg = f"restore returned unexpected seed: {seed_after!r}"
                raise RuntimeError(msg)
        finally:
            # Tear down the standalone restore fork directly.
            delete_bucket(s3, restored, force=True)
    finally:
        # Stage 7 — clean up everything: forks (revoke + delete), then workspace.
        if forks is not None:
            print("[orchestrator] teardown_forks")
            teardown_forks(s3, forks)
        print("[orchestrator] teardown_workspace")
        teardown_workspace(s3, ws)


if __name__ == "__main__":
    main()
