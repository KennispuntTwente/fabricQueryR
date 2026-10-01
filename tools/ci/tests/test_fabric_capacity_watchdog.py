import copy
import importlib.util
import json
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "fabric_capacity_watchdog.py"
SPEC = importlib.util.spec_from_file_location("watchdog", SCRIPT)
watchdog = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(watchdog)

RESOURCE = "/subscriptions/11111111-1111-1111-1111-111111111111/resourceGroups/test/providers/Microsoft.Fabric/capacities/testf2"
REPOSITORY = "example/fabricQueryR"
LEASE = "22222222-2222-2222-2222-222222222222"


def resource(state="Active", deadline=999):
    return {
        "id": RESOURCE,
        "sku": {"name": "F2"},
        "properties": {"state": state},
        "tags": {
            watchdog.OWNER_TAG: REPOSITORY,
            watchdog.LEASE_TAG: LEASE,
            watchdog.DEADLINE_TAG: str(deadline),
        },
    }


class Capacity:
    def __init__(self, *resources):
        self.resources = list(resources)
        self.current = resources[0]
        self.posts = 0
        self.lost_response = False

    def read(self):
        if self.resources:
            self.current = self.resources.pop(0)
        return copy.deepcopy(self.current)

    def suspend(self):
        self.posts += 1
        self.current["properties"]["state"] = "Paused"
        if self.lost_response:
            raise watchdog.AzureRequestError("response lost")


class WatchdogTests(unittest.TestCase):
    def check(self, capacity, **kwargs):
        return watchdog.check_capacity(
            capacity, REPOSITORY, now=lambda: 1000, sleep=lambda _: None, **kwargs
        )

    def test_paused_and_unexpired_capacities_are_untouched(self):
        for item in (resource("Paused"), resource(deadline=1100)):
            with self.subTest(item=item):
                capacity = Capacity(item)
                self.check(capacity)
                self.assertEqual(capacity.posts, 0)

    def test_expired_capacity_is_paused_and_next_check_is_a_noop(self):
        capacity = Capacity(resource(deadline=1000))
        self.assertIn("data retained", self.check(capacity))
        self.assertEqual(self.check(capacity), "F2 already paused")
        self.assertEqual(capacity.posts, 1)

    def test_new_session_is_not_paused_by_an_old_observation(self):
        for deadline in (900, 1100):
            with self.subTest(deadline=deadline):
                newer = resource(deadline=deadline)
                newer["tags"][watchdog.LEASE_TAG] = (
                    "33333333-3333-3333-3333-333333333333"
                )
                capacity = Capacity(resource(), newer)
                self.check(capacity)
                self.assertEqual(capacity.posts, 0)

    def test_invalid_owned_metadata_fails_closed(self):
        for key, value in (
            (watchdog.DEADLINE_TAG, "bad"),
            (watchdog.DEADLINE_TAG, "99999"),
            (watchdog.DEADLINE_TAG, None),
            (watchdog.LEASE_TAG, "bad"),
        ):
            with self.subTest(key=key, value=value):
                item = resource()
                item["tags"][key] = value
                capacity = Capacity(item)
                self.check(capacity)
                self.assertEqual(capacity.posts, 1)

    def test_unowned_active_resource_is_reported_without_mutation(self):
        item = resource()
        item["tags"] = {}
        capacity = Capacity(item)
        with self.assertRaisesRegex(RuntimeError, "no matching repository owner"):
            self.check(capacity)
        self.assertEqual(capacity.posts, 0)

    def test_resuming_and_pausing_transitions_are_handled(self):
        capacity = Capacity(
            resource("Resuming"), resource("Resuming"), resource("Active")
        )
        self.check(capacity)
        self.assertEqual(capacity.posts, 1)
        capacity = Capacity(
            resource("Pausing"), resource("Pausing"), resource("Paused")
        )
        self.check(capacity)
        self.assertEqual(capacity.posts, 0)

    def test_lost_suspend_response_does_not_repeat_accepted_mutation(self):
        capacity = Capacity(resource())
        capacity.lost_response = True
        self.check(capacity)
        self.assertEqual(capacity.posts, 1)

    def test_request_retries_and_transition_waits_are_bounded(self):
        capacity = Capacity(resource())
        with patch.object(
            capacity, "read", side_effect=watchdog.AzureRequestError("unavailable")
        ) as read:
            with self.assertRaises(watchdog.AzureRequestError):
                self.check(capacity)
            self.assertEqual(read.call_count, 3)
        capacity = Capacity(resource("Pausing"))
        with self.assertRaises(TimeoutError):
            self.check(capacity, attempts=3)
        self.assertEqual(capacity.posts, 0)

    def test_azure_transport_accepts_only_matching_f2_and_calls_suspend(self):
        capacity = watchdog.AzureCapacity(RESOURCE)
        completed = subprocess.CompletedProcess(
            [], 0, stdout=json.dumps(resource()), stderr=""
        )
        with patch.object(watchdog.subprocess, "run", return_value=completed) as run:
            self.assertEqual(capacity.read()["id"], RESOURCE)
            capacity.suspend()
        args = run.call_args_list[-1].args[0]
        self.assertIn("POST", args)
        self.assertIn(
            f"https://management.azure.com{RESOURCE}/suspend?api-version=2023-11-01",
            args,
        )
        for key, value in (("id", "another-resource"), ("sku", {"name": "F64"})):
            item = resource()
            item[key] = value
            with (
                patch.object(capacity, "request", return_value=item),
                self.assertRaises(RuntimeError),
            ):
                capacity.read()

    def test_invalid_resource_id_and_cli_authentication_errors_do_not_mutate(self):
        with self.assertRaises(ValueError):
            watchdog.AzureCapacity("https://other-host/capacity")
        capacity = watchdog.AzureCapacity(RESOURCE)
        completed = subprocess.CompletedProcess(
            [], 1, stdout="", stderr="sensitive diagnostic"
        )
        with (
            patch.object(watchdog.subprocess, "run", return_value=completed),
            self.assertRaises(watchdog.AzureRequestError) as error,
        ):
            capacity.read()
        self.assertNotIn("sensitive diagnostic", str(error.exception))


if __name__ == "__main__":
    unittest.main()
