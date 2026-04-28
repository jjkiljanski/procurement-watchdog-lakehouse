"""Update the Cloud Scheduler trigger with a new Spark container image.

Temporary helper until CI/CD automates this step.  Reads the current
scheduler body, patches container_image, and writes it back — no other
args are changed.

Usage:
    python scripts/ops/update_scheduler_image.py \\
        --image europe-central2-docker.pkg.dev/PROJECT/spark/procurement-spark:SHA \\
        --project procwatch-dev \\
        --region europe-central2
"""

from __future__ import annotations

import argparse
import base64
import json
import subprocess
import sys


def _gcloud(*args: str) -> str:
    result = subprocess.run(
        ["gcloud", *args],
        capture_output=True, text=True,
        shell=(sys.platform == "win32"),  # gcloud is a .cmd file on Windows
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)
    return result.stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description="Patch container_image in the daily Cloud Scheduler trigger.")
    parser.add_argument("--image", required=True, help="Full Spark image URI (e.g. .../procurement-spark:SHA)")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument("--region", required=True, help="Scheduler location / region")
    parser.add_argument("--job", default="bzp-daily-trigger", help="Scheduler job name")
    args = parser.parse_args()

    raw = _gcloud(
        "scheduler", "jobs", "describe", args.job,
        f"--location={args.region}",
        f"--project={args.project}",
        "--format=json",
    )
    job = json.loads(raw)

    body_bytes = base64.b64decode(job["httpTarget"]["body"])
    envelope = json.loads(body_bytes)
    workflow_args = json.loads(envelope["argument"])

    old_image = workflow_args.get("container_image", "<not set>")
    workflow_args["container_image"] = args.image

    new_body = json.dumps({"argument": json.dumps(workflow_args)})

    print(f"Updating '{args.job}' in {args.region}:")
    print(f"  old image: {old_image}")
    print(f"  new image: {args.image}")

    _gcloud(
        "scheduler", "jobs", "update", "http", args.job,
        f"--location={args.region}",
        f"--project={args.project}",
        f"--message-body={new_body}",
    )
    print("Done.")


if __name__ == "__main__":
    main()
