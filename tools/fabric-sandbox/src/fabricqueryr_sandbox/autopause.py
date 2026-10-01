"""Arm an independent GitHub shutdown run before resuming the shared F2."""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

import httpx

from .capacity import F2Capacity
from .credentials import get_credential
from .fabric_api import FabricApi

LEASE_TAG = "fabricqueryr-shiny-lease"
OWNER_TAG = "fabricqueryr-shiny-repository"
DEADLINE_TAG = "fabricqueryr-shiny-pause-at"
# Keep the original lease keys compatible with the deployed periodic watchdog.
PURPOSE_TAG = "fabricqueryr-capacity-purpose"
RUN_TAG = "fabricqueryr-capacity-run"
MAX_SECONDS = 3600
VERIFY_STEP = "Verify shutdown lease"
WAIT_STEP = "Wait for automatic pause"


def output(name, value):
    if path := os.environ.get("GITHUB_OUTPUT"):
        with Path(path).open("a", encoding="utf-8") as stream:
            stream.write(f"{name}={value}\n")


def deadline_text(deadline):
    return datetime.fromtimestamp(deadline, timezone.utc).isoformat()


def read_lease(resource, repository):
    tags = resource.get("tags") or {}
    if tags.get(OWNER_TAG) != repository:
        raise RuntimeError("Capacity has no shutdown lease owned by this repository")
    try:
        lease = str(UUID(tags[LEASE_TAG]))
        deadline = int(tags[DEADLINE_TAG])
    except (KeyError, ValueError, TypeError) as error:
        raise RuntimeError("Capacity shutdown lease is invalid") from error
    return lease, deadline


def verify_lease(capacity, repository, lease):
    current, deadline = read_lease(capacity.status(), repository)
    if current != lease:
        raise RuntimeError("Capacity now belongs to a different shutdown lease")
    if deadline > time.time() + MAX_SECONDS + 5:
        raise RuntimeError("Shutdown deadline exceeds the one-hour limit")
    return deadline


def arm_lease(
    capacity, repository, *, check=False, now=time.time, purpose="shiny", run="",
):
    if purpose not in {"shiny", "integration"}:
        raise ValueError("Unknown capacity lease purpose")
    if purpose == "integration" and not run:
        raise ValueError("Integration activation requires a run identity")
    resource = capacity.status()
    state = resource["properties"]["state"]
    if check and state not in {"Paused", "Suspended"}:
        raise RuntimeError("The shutdown check requires an already paused F2")
    if state == "Active":
        tags = resource.get("tags") or {}
        if tags.get(PURPOSE_TAG, "shiny") != purpose or (
            purpose == "integration" and tags.get(RUN_TAG) != run
        ):
            raise RuntimeError("F2 is in use by another session; refusing to borrow it")
        # Starting again must not buy another hour of an existing session.
        lease, deadline = read_lease(resource, repository)
        if not now() < deadline <= now() + MAX_SECONDS:
            raise RuntimeError(
                "Shutdown lease expired or invalid; run shiny-pause first"
            )
        return lease, deadline
    if state not in {"Paused", "Suspended"}:
        raise RuntimeError(f"Cannot arm shutdown while capacity is {state}")
    lease = str(uuid4())
    deadline = int(now()) + (120 if check else MAX_SECONDS)
    capacity.request(
        "PATCH",
        "/providers/Microsoft.Resources/tags/default",
        api_version="2021-04-01",
        json={
            "operation": "Merge",
            "properties": {
                "tags": {
                    LEASE_TAG: lease,
                    OWNER_TAG: repository,
                    DEADLINE_TAG: str(deadline),
                    PURPOSE_TAG: purpose,
                    RUN_TAG: run or "shiny",
                }
            },
        },
    )
    if read_lease(capacity.status(), repository) != (lease, deadline):
        raise RuntimeError("Azure did not retain the shutdown lease; refusing resume")
    return lease, deadline


class GitHubGuard:
    def __init__(
        self, repository, token, ref, sha, *, transport=None, sleep=time.sleep
    ):
        self.repository, self.ref, self.sha = repository, ref, sha
        self.sleep = sleep
        self.client = httpx.Client(
            base_url=f"https://api.github.com/repos/{repository}/actions/",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=30,
            transport=transport,
        )

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.client.close()

    def request(self, method, path, **kwargs):
        response = self.client.request(method, path, **kwargs)
        response.raise_for_status()
        return response

    def ready(self, run_id):
        run = self.request("GET", f"runs/{run_id}").json()
        if run["head_sha"] != self.sha or run["status"] == "completed":
            raise RuntimeError(
                "Independent shutdown run stopped or uses another revision"
            )
        jobs = self.request(
            "GET", f"runs/{run_id}/jobs", params={"per_page": 100}
        ).json()
        for job in jobs["jobs"]:
            steps = {step["name"]: step for step in job.get("steps", [])}
            if (
                steps.get(VERIFY_STEP, {}).get("conclusion") == "success"
                and steps.get(WAIT_STEP, {}).get("status") == "in_progress"
            ):
                return True
        return False

    def arm(self, lease, *, attempts=36):
        key = str(uuid4())
        workflow = "workflows/fabric-sandbox.yaml"
        self.request(
            "POST",
            f"{workflow}/dispatches",
            json={
                "ref": self.ref,
                "inputs": {
                    "action": "shiny-watchdog",
                    "lease_id": lease,
                    "guard_key": key,
                },
            },
        )
        title = f"Fabric sandbox - shiny-watchdog - {key}"
        run_id = None
        for _ in range(attempts):
            if run_id is None:
                runs = self.request(
                    "GET",
                    f"{workflow}/runs",
                    params={"event": "workflow_dispatch", "per_page": 100},
                ).json()["workflow_runs"]
                matches = [run for run in runs if run.get("display_title") == title]
                if len(matches) > 1:
                    raise RuntimeError("Ambiguous independent shutdown run")
                if matches:
                    run_id = matches[0]["id"]
            if run_id is not None and self.ready(run_id):
                return run_id
            self.sleep(5)
        raise TimeoutError("Shutdown job did not become ready; F2 will not be resumed")


def start(capacity, guard, *, check=False, now=time.time, purpose="shiny", run=""):
    lease, deadline = arm_lease(
        capacity, guard.repository, check=check, now=now, purpose=purpose, run=run,
    )
    # Publish ownership before dispatch so failure cleanup also covers arm errors.
    output("lease_id", lease)
    run_id = guard.arm(lease)
    output("guard_run_id", run_id)
    output("pause_at", deadline_text(deadline))
    print(f"Automatic pause: {deadline_text(deadline)}", flush=True)
    print(
        f"Shutdown run: https://github.com/{guard.repository}/actions/runs/{run_id}",
        flush=True,
    )
    if check:
        print("Shutdown check armed on paused F2; no resume requested", flush=True)
        return

    def before_resume():
        if verify_lease(capacity, guard.repository, lease) <= now() or not guard.ready(
            run_id
        ):
            raise RuntimeError("Independent shutdown is not armed; refusing resume")
        output("resumed", "true")

    capacity.change("resume", before_resume=before_resume)
    # Cancellation can race with the resume request. Fail closed if its guard
    # stopped between the preflight and the accepted resume.
    try:
        if deadline <= now() or not guard.ready(run_id):
            raise RuntimeError("Independent shutdown stopped during resume")
    except Exception:
        pause_lease(capacity, guard.repository, lease)
        raise
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as stream:
            stream.write(
                f"Automatic F2 pause: {deadline_text(deadline)} (including setup time).\n\n"
                f"[Independent shutdown run](https://github.com/{guard.repository}/actions/runs/{run_id})\n\n"
            )


def pause_lease(capacity, repository, lease, *, attempts=3, sleep=time.sleep):
    for attempt in range(attempts):
        try:
            resource = capacity.status()
            tags = resource.get("tags") or {}
            if tags.get(OWNER_TAG) != repository or tags.get(LEASE_TAG) != lease:
                print(
                    "Shutdown lease was replaced; leaving the current session alone",
                    flush=True,
                )
                return False
            changed = capacity.change("pause")
            print("F2 is paused; workspace data retained", flush=True)
            return changed
        except httpx.HTTPError:
            if attempt + 1 == attempts:
                raise
            # Re-read both state and ownership before retrying an ambiguous POST.
            print("Azure shutdown request failed; retrying in 10 seconds", flush=True)
            sleep(10)


def wait_deadline(deadline, *, now=time.time, sleep=time.sleep):
    if deadline > now() + MAX_SECONDS + 5:
        raise ValueError("Refusing to wait longer than one hour")
    print(f"Waiting until {deadline_text(deadline)}", flush=True)
    while (remaining := deadline - now()) > 0:
        sleep(min(30, remaining))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "check", "verify", "wait", "pause"))
    parser.add_argument("--resource-id")
    parser.add_argument("--capacity-id")
    parser.add_argument("--lease-id", type=lambda value: str(UUID(value)))
    parser.add_argument("--deadline", type=int)
    parser.add_argument("--purpose", choices=("shiny", "integration"), default="shiny")
    parser.add_argument("--run", default="")
    args = parser.parse_args(argv)
    if args.action == "wait":
        if args.deadline is None:
            parser.error("--deadline is required")
        wait_deadline(args.deadline)
        return
    repository = os.environ.get("GITHUB_REPOSITORY")
    if not repository or not args.resource_id or not args.capacity_id:
        parser.error("GITHUB_REPOSITORY, --resource-id and --capacity-id are required")
    if args.action in {"verify", "pause"} and not args.lease_id:
        parser.error("--lease-id is required")
    credential = get_credential()
    with F2Capacity(args.resource_id, args.capacity_id, credential) as capacity:
        if args.action in {"start", "check"}:
            for name in ("GITHUB_TOKEN", "GITHUB_REF_NAME", "GITHUB_SHA"):
                if not os.environ.get(name):
                    parser.error(f"{name} is required")
            with FabricApi(credential) as api:
                capacity.verify_fabric_identity(api)
            with GitHubGuard(
                repository,
                os.environ["GITHUB_TOKEN"],
                os.environ["GITHUB_REF_NAME"],
                os.environ["GITHUB_SHA"],
            ) as guard:
                start(
                    capacity, guard, check=args.action == "check",
                    purpose=args.purpose, run=args.run,
                )
        elif args.action == "verify":
            deadline = verify_lease(capacity, repository, args.lease_id)
            output("deadline", deadline)
            print(f"Verified shutdown lease; pause at {deadline_text(deadline)}")
        else:
            pause_lease(capacity, repository, args.lease_id)


if __name__ == "__main__":
    main()
