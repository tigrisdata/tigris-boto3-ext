"""Run the multi-agent agent_kit workflow on real sprites.dev sprites.

Same flow as ``examples/multi_agent_workflow.py`` but each "trial agent"
runs inside its own sprites.dev microVM instead of a thread. The
orchestrator:

  1. Builds a workspace + N forks with scoped Editor credentials via
     ``create_workspace`` / ``create_forks``.
  2. For each fork: spins up a sprite, installs boto3, uploads
     ``examples/sprite_agent.py``, runs it with the fork's scoped
     credentials passed in as env vars.
  3. Reads each fork bucket to collect results.
  4. Destroys every sprite and tears down the agent_kit resources.

Prereqs:

  - The ``sprite`` CLI installed and authed (``sprite login``).
  - Tigris creds in env: ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``,
    ``AWS_ENDPOINT_URL_S3``.

Run::

    uv run python examples/sprites_multi_agent_test.py
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import boto3

from tigris_boto3_ext import (
    Fork,
    ForkSet,
    Workspace,
    create_forks,
    create_workspace,
    teardown_forks,
    teardown_workspace,
)

NUM_TRIALS = 4
SEED_KEY = "input/seed.txt"
RESULT_KEY = "output/result.txt"

AGENT_SCRIPT = Path(__file__).parent / "sprite_agent.py"


def _orchestrator_client() -> Any:
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_ENDPOINT_URL")
        or "https://t3.storage.dev",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_REGION", "auto"),
    )


_REDACT_PREFIXES = (
    "AWS_ACCESS_KEY_ID=",
    "AWS_SECRET_ACCESS_KEY=",
    "AWS_SESSION_TOKEN=",
)


def _redact(arg: str) -> str:
    """Hide credential values when echoing the command line.

    Handles both standalone ``KEY=value`` args and the
    ``KEY=value,KEY2=value2`` form ``sprite exec --env`` expects.
    """
    parts = arg.split(",")
    redacted = []
    changed = False
    for part in parts:
        for prefix in _REDACT_PREFIXES:
            if part.startswith(prefix):
                redacted.append(f"{prefix}<redacted>")
                changed = True
                break
        else:
            redacted.append(part)
    return ",".join(redacted) if changed else arg


def _sprite(*args: str, capture: bool = False) -> subprocess.CompletedProcess:
    """Invoke the ``sprite`` CLI with logging. Raises on non-zero exit."""
    cmd = ["sprite", *args]
    safe = " ".join(shlex.quote(_redact(a)) for a in cmd)
    print(f"  $ {safe}")
    return subprocess.run(  # noqa: S603
        cmd,
        check=True,
        capture_output=capture,
        text=True,
    )


def provision_sprite(name: str) -> None:
    """Create a sprite and install boto3 inside it."""
    _sprite("create", name, "--skip-console")
    # Sprites ship with Python; install boto3 once per sprite.
    # `--` separates sprite-cli flags from the command's flags so `bash -c`
    # isn't reinterpreted as a sprite flag.
    # Use Debian's packaged boto3 — pip3 in --user mode left transitive
    # deps off Python 3.13's sys.path on the default sprite image, so apt
    # is more reliable. Sprite exec runs as a non-root user, so sudo.
    _sprite(
        "exec",
        "-s",
        name,
        "--",
        "bash",
        "-c",
        "sudo apt-get update -qq && sudo apt-get install -y -qq python3-boto3",
    )


def run_agent_on_sprite(name: str, fork: Fork, trial_id: int) -> None:
    """Upload the agent script and run it inside ``name`` with scoped creds."""
    if fork.credentials is None:
        msg = f"fork {fork.bucket!r} has no scoped credentials"
        raise RuntimeError(msg)

    endpoint = (
        os.environ.get("AWS_ENDPOINT_URL_S3")
        or os.environ.get("AWS_ENDPOINT_URL")
        or "https://t3.storage.dev"
    )

    # ``sprite exec --env`` takes a single comma-separated KEY=value list,
    # not repeated flags. Tigris creds don't contain commas; fail fast if
    # that ever changes.
    env_pairs = [
        f"AWS_ENDPOINT_URL_S3={endpoint}",
        f"AWS_ACCESS_KEY_ID={fork.credentials.access_key_id}",
        f"AWS_SECRET_ACCESS_KEY={fork.credentials.secret_access_key}",
        f"AWS_REGION={os.environ.get('AWS_REGION', 'auto')}",
        f"FORK_BUCKET={fork.bucket}",
        f"TRIAL_ID={trial_id}",
    ]
    if any("," in p for p in env_pairs):
        msg = "env value contains a comma; sprite --env can't represent it"
        raise RuntimeError(msg)

    _sprite(
        "exec",
        "-s",
        name,
        "--file",
        f"{AGENT_SCRIPT}:/tmp/agent.py",
        "--env",
        ",".join(env_pairs),
        "--",
        "python3",
        "/tmp/agent.py",  # noqa: S108
    )


def destroy_sprite(name: str) -> None:
    try:
        _sprite("destroy", name, "--force")
    except subprocess.CalledProcessError as exc:
        print(f"  ! warning: failed to destroy sprite {name!r}: {exc}", file=sys.stderr)


def main() -> None:
    if not AGENT_SCRIPT.exists():
        msg = f"missing agent script at {AGENT_SCRIPT}"
        raise RuntimeError(msg)

    s3 = _orchestrator_client()
    run_id = uuid.uuid4().hex[:8]

    workspace_name = f"akwf-{run_id}"
    print(f"[orchestrator] create_workspace({workspace_name})")
    ws: Workspace = create_workspace(s3, workspace_name)
    s3.put_object(Bucket=ws.bucket, Key=SEED_KEY, Body=b"hello-from-orchestrator")

    forks: ForkSet | None = None
    sprite_names: list[str] = []
    try:
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

        for i, fork in enumerate(forks.forks):
            sprite_name = f"akwf-{run_id}-{i}"
            sprite_names.append(sprite_name)
            print(f"[orchestrator] provisioning sprite {sprite_name!r}...")
            provision_sprite(sprite_name)
            print(f"[orchestrator] running trial {i} on {sprite_name!r}...")
            run_agent_on_sprite(sprite_name, fork, i)

        print("[orchestrator] collecting results from fork buckets:")
        for i, fork in enumerate(forks.forks):
            body = (
                s3.get_object(Bucket=fork.bucket, Key=RESULT_KEY)["Body"]
                .read()
                .decode()
            )
            print(f"[orchestrator]   {i}: {body}")
    finally:
        for name in sprite_names:
            print(f"[orchestrator] destroy sprite {name!r}")
            destroy_sprite(name)
        if forks is not None:
            print("[orchestrator] teardown_forks")
            teardown_forks(s3, forks)
        print("[orchestrator] teardown_workspace")
        teardown_workspace(s3, ws)


if __name__ == "__main__":
    main()
