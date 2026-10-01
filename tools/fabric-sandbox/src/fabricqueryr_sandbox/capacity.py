"""Control an existing F2 capacity; never create or resize paid resources."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from pathlib import Path

import httpx

from .credentials import get_credential
from .fabric_api import FabricApi

ARM_SCOPE = "https://management.azure.com/.default"
ARM_VERSION = "2023-11-01"
RESOURCE_PATTERN = re.compile(
    r"/subscriptions/[0-9a-f-]{36}/resourceGroups/[^/?#]+/"
    r"providers/Microsoft\.Fabric/capacities/[a-z][a-z0-9]{2,62}",
    re.IGNORECASE,
)


class F2Capacity:
    def __init__(
        self,
        resource_id,
        capacity_id,
        credential,
        *,
        transport=None,
        sleep=time.sleep,
        attempts=60,
    ):
        if not RESOURCE_PATTERN.fullmatch(resource_id):
            raise ValueError("Expected an Azure Microsoft.Fabric capacity resource ID")
        self.resource_id = resource_id
        self.capacity_id = capacity_id
        self.credential = credential
        self.sleep = sleep
        self.attempts = attempts
        self.client = httpx.Client(
            base_url="https://management.azure.com", transport=transport, timeout=60
        )

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.client.close()

    def request(self, method, suffix=""):
        response = self.client.request(
            method,
            self.resource_id + suffix,
            params={"api-version": ARM_VERSION},
            headers={
                "Authorization": f"Bearer {self.credential.get_token(ARM_SCOPE).token}"
            },
        )
        response.raise_for_status()
        return response

    def status(self):
        resource = self.request("GET").json()
        if resource.get("sku", {}).get("name") != "F2":
            raise RuntimeError(
                "Refusing capacity control: the Azure resource is not F2"
            )
        if resource.get("id", "").casefold() != self.resource_id.casefold():
            raise RuntimeError("Azure returned a different capacity resource")
        return resource

    def verify_fabric_identity(self, api):
        path = "/capacities"
        matches = []
        while path:
            page = api.request("GET", path).json()
            matches.extend(
                item
                for item in page.get("value", [])
                if item["id"].casefold() == self.capacity_id.casefold()
            )
            path = page.get("continuationUri")
        if len(matches) != 1 or matches[0].get("sku") != "F2":
            raise RuntimeError(
                "The configured Fabric capacity ID is not an accessible F2"
            )
        if (
            matches[0].get("displayName", "").casefold()
            != self.resource_id.rsplit("/", 1)[1].casefold()
        ):
            raise RuntimeError("The Azure resource and Fabric capacity ID do not match")
        return matches[0]

    def change(self, action, *, before_resume=lambda: None):
        if action not in {"resume", "pause"}:
            raise ValueError("Capacity action must be resume or pause")
        resource = self.status()
        state = resource["properties"]["state"]
        if action == "pause" and state in {"Resuming", "Pausing"}:
            for _ in range(self.attempts):
                self.sleep(10)
                resource = self.status()
                state = resource["properties"]["state"]
                if state not in {"Resuming", "Pausing"}:
                    break
        desired = {"Active"} if action == "resume" else {"Paused", "Suspended"}
        if state in desired:
            return False
        permitted = {"Paused", "Suspended"} if action == "resume" else {"Active"}
        if state not in permitted:
            raise RuntimeError(
                f"Capacity is transitioning or unavailable ({state}); retry later"
            )
        if action == "resume":
            # Record ownership before POST so failure cleanup also handles an
            # accepted resume whose response or subsequent polling is interrupted.
            before_resume()
        self.request("POST", "/resume" if action == "resume" else "/suspend")
        for _ in range(self.attempts):
            resource = self.status()
            if resource["properties"]["state"] in desired:
                return True
            if resource["properties"].get("provisioningState") == "Failed":
                raise RuntimeError("Azure capacity operation failed")
            self.sleep(10)
        raise TimeoutError(
            f"Capacity did not finish {action}; check Azure before retrying"
        )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "resume", "pause"))
    parser.add_argument("--resource-id", required=True)
    parser.add_argument("--capacity-id", required=True)
    args = parser.parse_args(argv)
    credential = get_credential()

    def mark_resumed():
        if output := os.environ.get("GITHUB_OUTPUT"):
            with Path(output).open("a", encoding="utf-8") as stream:
                stream.write("resumed=true\n")

    with F2Capacity(args.resource_id, args.capacity_id, credential) as capacity:
        resource = capacity.status()
        # ARM control still works while Fabric data-plane endpoints are suspended.
        # Resume additionally checks the exact configured Fabric capacity identity.
        if args.action != "pause":
            with FabricApi(credential) as api:
                capacity.verify_fabric_identity(api)
        if args.action != "status":
            capacity.change(args.action, before_resume=mark_resumed)
            resource = capacity.status()
        print(
            json.dumps(
                {
                    "name": resource["name"],
                    "sku": resource["sku"]["name"],
                    "state": resource["properties"]["state"],
                }
            )
        )
        if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
            with Path(summary).open("a", encoding="utf-8") as stream:
                stream.write(
                    f"F2 capacity `{resource['name']}`: {resource['properties']['state']}.\n\n"
                )


if __name__ == "__main__":
    main()
