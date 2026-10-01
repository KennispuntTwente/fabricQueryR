"""Inspect or prepare an interactive sandbox while retaining its workspace."""

from __future__ import annotations

import argparse
import os
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import httpx
from azure.storage.filedatalake import DataLakeServiceClient

from . import persistent_ci, shiny_sandbox
from .credentials import CachedTokenCredential, get_credential
from .fabric_api import FabricApi
from .persistent_workspace import WORKSPACES, ensure_workspace, find_workspace
from .settings import SandboxSettings


def prepare(api, credential, settings, repository, owner, sandbox, *, reseed=False):
    shiny_sandbox.wait_active_capacity(api, settings.capacity_id)
    workspace = ensure_workspace(
        api,
        WORKSPACES[sandbox],
        settings.capacity_id,
        repository,
        owner,
        assign_capacity=True,
    )
    if sandbox == "shiny":
        shiny_sandbox.prepare_workspace(
            api,
            credential,
            workspace,
            repository,
            owner,
            settings.repository_root,
            reseed=reseed,
        )
    else:
        shiny_sandbox.ensure_owner(api, workspace["id"], owner)
        settings = replace(
            settings,
            workspace_id=workspace["id"],
            workspace_name=workspace["displayName"],
            spark_runtime_lane="preview",
            spark_runtime_version="2.0",
        )
        with DataLakeServiceClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            credential=credential,
        ) as service:
            try:
                persistent_ci.reconcile(
                    api, credential, service, settings, reseed=reseed
                )
            except httpx.HTTPStatusError as error:
                # Preserve the old development sandbox's optional SQL DB fallback.
                try:
                    code = error.response.json().get("errorCode")
                except (ValueError, AttributeError):
                    raise error
                if (
                    not settings.provision_sql_database
                    or code != "SqlDatabasePerCapacityLimitReached"
                    or not error.request.url.path.endswith("/sqlDatabases")
                ):
                    raise
                print(
                    "SQL Database capacity limit reached; keeping the other fixtures",
                    flush=True,
                )
                persistent_ci.reconcile(
                    api,
                    credential,
                    service,
                    replace(settings, provision_sql_database=False),
                    reseed=reseed,
                )
    return workspace


def summary(sandbox, workspace, capacity_id, *, ready=False):
    name = WORKSPACES[sandbox]
    lines = [f"## {sandbox.capitalize()} sandbox", "", f"Workspace: `{name}`.", ""]
    if workspace is None:
        lines += ["Workspace has not been created. Select `start` to prepare it.", ""]
    else:
        lines += [
            f"[Open workspace](https://app.fabric.microsoft.com/groups/{workspace['id']}/list)",
            "",
            "Assigned to the configured F2."
            if workspace.get("capacityId") == capacity_id
            else "Start will assign this owned workspace to the configured F2.",
            "",
        ]
    if ready:
        launch = (
            "Sys.setenv(FABRIC_SHINY_WORKSPACE = 'fabricqueryr-shiny-dhrkoning')\n"
            "shiny::runApp('playground/shiny', port = 8100)"
            if sandbox == "shiny"
            else 'source("playground/sandbox.R")\nsandbox <- connect_playground_sandbox()'
        )
        lines += [
            "Ready for interactive use until the capacity session deadline.",
            "",
            "```r",
            launch,
            "```",
            "",
            "Select `pause` with this sandbox to finish early. Its workspace and data are retained.",
            "",
        ]
    text = "\n".join(lines)
    print(text, flush=True)
    if destination := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(destination).open("a", encoding="utf-8") as stream:
            stream.write(text + "\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("status", "prepare"))
    parser.add_argument("--sandbox", choices=WORKSPACES, required=True)
    parser.add_argument(
        "--capacity-id", required=True, type=lambda value: str(UUID(value))
    )
    parser.add_argument(
        "--owner-id", required=True, type=lambda value: str(UUID(value))
    )
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--reseed", action="store_true")
    args = parser.parse_args(argv)
    if not args.repository:
        parser.error("--repository or GITHUB_REPOSITORY is required")
    if args.reseed and args.action != "prepare":
        parser.error("--reseed is only valid with prepare")
    credential = CachedTokenCredential(get_credential())
    with FabricApi(credential) as api:
        if args.action == "status":
            workspace = find_workspace(
                api, WORKSPACES[args.sandbox], args.repository, args.owner_id
            )
        else:
            settings = replace(
                SandboxSettings.from_environment(), capacity_id=args.capacity_id
            )
            workspace = prepare(
                api,
                credential,
                settings,
                args.repository,
                args.owner_id,
                args.sandbox,
                reseed=args.reseed,
            )
    summary(args.sandbox, workspace, args.capacity_id, ready=args.action == "prepare")


if __name__ == "__main__":
    main()
