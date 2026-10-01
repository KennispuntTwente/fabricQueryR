"""Pause an overdue integration or Shiny F2 without its startup workflow."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from uuid import UUID

OWNER_TAG = "fabricqueryr-shiny-repository"
LEASE_TAG = "fabricqueryr-shiny-lease"
DEADLINE_TAG = "fabricqueryr-shiny-pause-at"
RESOURCE_PATTERN = re.compile(
    r"/subscriptions/[0-9a-f-]{36}/resourceGroups/[A-Za-z0-9_.()-]+/"
    r"providers/Microsoft\.Fabric/capacities/[a-z][a-z0-9]{2,62}",
    re.IGNORECASE,
)


class AzureRequestError(RuntimeError):
    pass


class AzureCapacity:
    """Azure CLI manages authentication; this client only reads and suspends."""

    def __init__(self, resource_id):
        if not RESOURCE_PATTERN.fullmatch(resource_id):
            raise ValueError("Expected an Azure Microsoft.Fabric capacity resource ID")
        self.resource_id = resource_id

    def request(self, method, suffix=""):
        result = subprocess.run(
            [
                shutil.which("az") or "az",
                "rest",
                "--method",
                method,
                "--url",
                f"https://management.azure.com{self.resource_id}{suffix}?api-version=2023-11-01",
                "--output",
                "json",
                "--only-show-errors",
            ],
            capture_output=True,
            text=True,
            timeout=90,
            check=False,
        )
        if result.returncode:
            # Do not forward authentication diagnostics or credential values.
            raise AzureRequestError(
                f"Azure capacity {method} request failed (exit {result.returncode})"
            )
        return json.loads(result.stdout) if result.stdout.strip() else {}

    def read(self):
        resource = self.request("GET")
        if resource.get("id", "").casefold() != self.resource_id.casefold():
            raise RuntimeError("Azure returned a different capacity resource")
        if resource.get("sku", {}).get("name") != "F2":
            raise RuntimeError(
                "Refusing automatic pause: configured resource is not F2"
            )
        return resource

    def suspend(self):
        self.request("POST", "/suspend")


def due(resource, repository, now):
    tags = resource.get("tags") or {}
    if tags.get(OWNER_TAG, "").casefold() != repository.casefold():
        raise RuntimeError(
            "Active F2 has no matching repository owner tag; inspect it manually"
        )
    fingerprint = tuple(tags.get(key) for key in (OWNER_TAG, LEASE_TAG, DEADLINE_TAG))
    try:
        UUID(tags[LEASE_TAG])
        deadline = int(tags[DEADLINE_TAG])
        if deadline <= 0 or deadline > now + 3605:
            raise ValueError("Deadline outside the one-hour window")
    except (KeyError, ValueError, TypeError, AttributeError):
        return fingerprint, True, "invalid shutdown metadata"
    return (
        fingerprint,
        deadline <= now,
        "expired deadline" if deadline <= now else "deadline not reached",
    )


def check_capacity(
    capacity, repository, *, now=time.time, sleep=time.sleep, attempts=30
):
    observed = None
    submitted = False
    failures = 0
    for _ in range(attempts):
        try:
            resource = capacity.read()
            state = resource["properties"]["state"]
            if state in {"Paused", "Suspended"}:
                return (
                    "F2 paused; workspace data retained"
                    if submitted
                    else "F2 already paused"
                )
            if state not in {"Active", "Resuming", "Pausing"}:
                raise RuntimeError(
                    f"Cannot automatically pause capacity in state {state}"
                )
            fingerprint, expired, reason = due(resource, repository, now())
            if not expired:
                return "Current capacity session is still within its deadline"
            if observed is None:
                # Read again immediately before deciding to suspend. A new session
                # or updated deadline must not inherit an older run's decision.
                observed = fingerprint
                continue
            if fingerprint != observed:
                return "Capacity session changed during this check; leaving it alone"
            if state == "Active" and not submitted:
                print(f"Pausing F2: {reason}", flush=True)
                capacity.suspend()
                submitted = True
        except (AzureRequestError, subprocess.TimeoutExpired):
            failures += 1
            if failures >= 3:
                raise
            print("Azure request failed; rechecking state before retry", flush=True)
        sleep(10)
    raise TimeoutError("F2 did not finish pausing; the next periodic run will retry")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resource-id", required=True)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    args = parser.parse_args(argv)
    if not args.repository:
        parser.error("--repository or GITHUB_REPOSITORY is required")
    result = check_capacity(AzureCapacity(args.resource_id), args.repository)
    print(result)
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as stream:
            stream.write(f"{result}.\n")


if __name__ == "__main__":
    main()
