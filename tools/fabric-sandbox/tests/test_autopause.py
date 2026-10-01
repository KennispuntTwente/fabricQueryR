import json
import runpy
import time
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from azure.core.credentials import AccessToken
from fabricqueryr_sandbox import autopause
from fabricqueryr_sandbox.capacity import F2Capacity
from fabricqueryr_sandbox.capacity import main as capacity_main

RESOURCE = "/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/test/providers/Microsoft.Fabric/capacities/testf2"
CAPACITY = "22222222-2222-2222-2222-222222222222"
REPOSITORY = "example/fabricQueryR"
LEASE = "33333333-3333-3333-3333-333333333333"


class Credential:
    def get_token(self, *_args, **_kwargs):
        return AccessToken("test-token", 4_102_444_800)


class Azure:
    def __init__(self, state="Paused", *, lease=None, deadline=None):
        self.resource = {
            "id": RESOURCE,
            "name": "testf2",
            "sku": {"name": "F2"},
            "properties": {"state": state, "provisioningState": "Succeeded"},
            "tags": {"existing": "keep-me"},
        }
        self.requests = []
        if lease:
            self.resource["tags"].update(
                {
                    autopause.LEASE_TAG: lease,
                    autopause.OWNER_TAG: REPOSITORY,
                    autopause.DEADLINE_TAG: str(deadline),
                }
            )

    def handle(self, request):
        self.requests.append(request)
        if request.method == "PATCH":
            assert request.url.path.endswith(
                "/providers/Microsoft.Resources/tags/default"
            )
            assert request.url.params["api-version"] == "2021-04-01"
            body = json.loads(request.content)
            assert body["operation"] == "Merge"
            self.resource["tags"].update(body["properties"]["tags"])
        elif request.method == "POST":
            self.resource["properties"]["state"] = (
                "Active" if request.url.path.endswith("/resume") else "Paused"
            )
        return httpx.Response(200, json=self.resource)

    def client(self):
        return F2Capacity(
            RESOURCE, CAPACITY, Credential(), transport=httpx.MockTransport(self.handle)
        )

    def mutations(self):
        return [request for request in self.requests if request.method != "GET"]


class Guard:
    repository = REPOSITORY

    def __init__(self, azure, *, ready=True, fail_arm=False):
        self.azure, self.is_ready, self.fail_arm = azure, ready, fail_arm
        self.arm_states = []

    def arm(self, lease):
        assert self.azure.resource["tags"][autopause.LEASE_TAG] == lease
        self.arm_states.append(self.azure.resource["properties"]["state"])
        if self.fail_arm:
            raise TimeoutError("guard unavailable")
        return 123

    def ready(self, run_id):
        assert run_id == 123
        return self.is_ready


def test_start_persists_one_hour_deadline_and_arms_guard_before_resume(
    tmp_path, monkeypatch
):
    azure = Azure()
    guard = Guard(azure)
    output = tmp_path / "outputs"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    now = int(time.time())
    with azure.client() as capacity:
        autopause.start(capacity, guard, now=lambda: now)
    assert guard.arm_states == ["Paused"]
    assert azure.resource["properties"]["state"] == "Active"
    assert azure.resource["tags"][autopause.DEADLINE_TAG] == str(now + 3600)
    assert azure.resource["tags"]["existing"] == "keep-me"
    assert "resumed=true" in output.read_text()
    assert [request.method for request in azure.mutations()] == ["PATCH", "POST"]


@pytest.mark.parametrize("fail_arm", [False, True])
def test_start_never_resumes_if_guard_is_unavailable(fail_arm):
    azure = Azure()
    guard = Guard(azure, ready=False, fail_arm=fail_arm)
    with azure.client() as capacity, pytest.raises((RuntimeError, TimeoutError)):
        autopause.start(capacity, guard)
    assert azure.resource["properties"]["state"] == "Paused"
    assert not any(request.method == "POST" for request in azure.requests)


def test_repeated_start_reuses_deadline_without_tag_write_or_resume():
    deadline = int(time.time()) + 1200
    azure = Azure("Active", lease=LEASE, deadline=deadline)
    with azure.client() as capacity:
        autopause.start(capacity, Guard(azure))
    assert azure.resource["tags"][autopause.DEADLINE_TAG] == str(deadline)
    assert not azure.mutations()


def test_periodic_guard_understands_the_startup_lease_without_extending_it():
    watchdog = runpy.run_path(
        str(Path(__file__).resolve().parents[2] / "ci/fabric_capacity_watchdog.py")
    )
    azure = Azure()
    now = int(time.time())
    with azure.client() as capacity:
        lease, deadline = autopause.arm_lease(capacity, REPOSITORY, now=lambda: now)
    fingerprint, expired, _ = watchdog["due"](azure.resource, REPOSITORY, now + 3599)
    assert fingerprint[1] == lease
    assert expired is False
    assert watchdog["due"](azure.resource, REPOSITORY, deadline)[1] is True


@pytest.mark.parametrize("offset", [-1, 7200, None])
def test_active_capacity_with_missing_expired_or_excessive_lease_is_rejected(offset):
    azure = Azure(
        "Active",
        lease=LEASE if offset is not None else None,
        deadline=int(time.time()) + (offset or 0),
    )
    with azure.client() as capacity, pytest.raises(RuntimeError):
        autopause.start(capacity, Guard(azure))
    assert not azure.mutations()


def test_guard_cancellation_during_resume_immediately_pauses():
    azure = Azure()
    guard = Guard(azure)
    guard.ready = lambda _: azure.resource["properties"]["state"] == "Paused"
    with (
        azure.client() as capacity,
        pytest.raises(RuntimeError, match="stopped during resume"),
    ):
        autopause.start(capacity, guard)
    assert azure.resource["properties"]["state"] == "Paused"
    assert [request.url.path.rsplit("/", 1)[1] for request in azure.mutations()] == [
        "default",
        "resume",
        "suspend",
    ]


@pytest.mark.parametrize("replaced", [False, True])
def test_expired_guard_pauses_only_its_own_session(replaced):
    azure = Azure(
        "Active",
        lease=str(uuid4()) if replaced else LEASE,
        deadline=int(time.time()) - 1,
    )
    with azure.client() as capacity:
        assert autopause.pause_lease(capacity, REPOSITORY, LEASE) is (not replaced)
    assert azure.resource["properties"]["state"] == ("Active" if replaced else "Paused")


def test_shutdown_check_runs_a_short_timer_without_any_capacity_post():
    azure = Azure()
    now = int(time.time())
    with azure.client() as capacity:
        autopause.start(capacity, Guard(azure), check=True, now=lambda: now)
        assert (
            autopause.pause_lease(
                capacity, REPOSITORY, azure.resource["tags"][autopause.LEASE_TAG]
            )
            is False
        )
    assert azure.resource["tags"][autopause.DEADLINE_TAG] == str(now + 120)
    assert not any(request.method == "POST" for request in azure.requests)


def test_shutdown_recovers_from_an_accepted_post_with_a_lost_response():
    azure = Azure("Active", lease=LEASE, deadline=int(time.time()) - 1)
    original = azure.handle
    sleeps = []

    def handle(request):
        response = original(request)
        if request.method == "POST":
            raise httpx.ReadTimeout("response lost", request=request)
        return response

    azure.handle = handle
    with azure.client() as capacity:
        autopause.pause_lease(capacity, REPOSITORY, LEASE, sleep=sleeps.append)
    assert azure.resource["properties"]["state"] == "Paused"
    assert sleeps == [10]
    assert sum(request.method == "POST" for request in azure.requests) == 1


def test_shutdown_check_refuses_an_active_capacity():
    azure = Azure("Active")
    with (
        azure.client() as capacity,
        pytest.raises(RuntimeError, match="already paused"),
    ):
        autopause.start(capacity, Guard(azure), check=True)
    assert not azure.mutations()


def test_timer_uses_absolute_deadline_and_wakes_at_most_every_30_seconds():
    clock = [1000]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += seconds

    autopause.wait_deadline(1065, now=lambda: clock[0], sleep=sleep)
    assert sleeps == [30, 30, 5]
    autopause.wait_deadline(1060, now=lambda: clock[0], sleep=sleep)
    assert sleeps == [30, 30, 5]
    with pytest.raises(ValueError, match="one hour"):
        autopause.wait_deadline(10000, now=lambda: clock[0], sleep=sleep)


def test_unprotected_capacity_resume_cli_is_disabled():
    with pytest.raises(SystemExit) as error:
        capacity_main(["resume", "--resource-id", RESOURCE, "--capacity-id", CAPACITY])
    assert error.value.code == 2


@pytest.mark.parametrize("result", ["ready", "failed", "queued", "wrong-revision"])
def test_github_dispatch_requires_verified_running_timer(result):
    requests = []
    inputs = {}

    def handle(request):
        requests.append(request)
        path = request.url.path
        if path.endswith("/dispatches"):
            body = json.loads(request.content)
            assert body["ref"] == "shiny-integration"
            inputs.update(body["inputs"])
            assert inputs["action"] == "shiny-watchdog" and inputs["lease_id"] == LEASE
            return httpx.Response(204)
        if path.endswith("/workflows/fabric-sandbox.yaml/runs"):
            return httpx.Response(
                200,
                json={
                    "workflow_runs": [
                        {
                            "id": 123,
                            "display_title": f"Fabric sandbox - shiny-watchdog - {inputs['guard_key']}",
                        }
                    ]
                },
            )
        if path.endswith("/runs/123"):
            return httpx.Response(
                200,
                json={
                    "status": "completed" if result == "failed" else "in_progress",
                    "head_sha": "other-sha"
                    if result == "wrong-revision"
                    else "tested-sha",
                },
            )
        if path.endswith("/runs/123/jobs"):
            return httpx.Response(
                200,
                json={
                    "jobs": [
                        {
                            "steps": [
                                {
                                    "name": autopause.VERIFY_STEP,
                                    "conclusion": "success",
                                },
                                {
                                    "name": autopause.WAIT_STEP,
                                    "status": "queued"
                                    if result == "queued"
                                    else "in_progress",
                                },
                            ]
                        }
                    ]
                },
            )
        raise AssertionError(path)

    with autopause.GitHubGuard(
        REPOSITORY,
        "github-token",
        "shiny-integration",
        "tested-sha",
        transport=httpx.MockTransport(handle),
        sleep=lambda _: None,
    ) as guard:
        if result == "ready":
            assert guard.arm(LEASE, attempts=2) == 123
        else:
            with pytest.raises((RuntimeError, TimeoutError)):
                guard.arm(LEASE, attempts=2)
    assert sum(request.method == "POST" for request in requests) == 1
