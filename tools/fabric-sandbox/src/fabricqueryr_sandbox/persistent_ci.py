"""Reconcile dedicated integration fixtures without destroying their workspaces."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import replace
from hashlib import sha256

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient

from .credentials import CachedTokenCredential, get_credential
from .deployment_revision import (
    deployment_items,
    deployment_revision,
    stale_deployments,
)
from .deploy import deploy
from .discover import discover, discover_onelake
from .fabric_api import FabricApi
from .fixture_revision import (
    INCOMPLETE_FIXTURE_REVISION,
    fixture_revision,
    read_fixture_contract,
    write_fixture_revision,
)
from .power_bi_api import (
    ARROW_SEMANTIC_MODEL_NAME,
    SEMANTIC_MODEL_NAME,
    prepare_arrow_test_semantic_model,
)
from .seed import seed
from .settings import SandboxSettings
from .shiny_sandbox import (
    complete_operation,
    definition_part,
    ensure_item,
    wait_active_capacity,
)

MANAGER = ".github/workflows/integration-fabric.yaml"


def workspace_identity(repository, lane):
    if not repository or ";" in repository or lane not in {"core", "preview"}:
        raise ValueError("A repository and core/preview lane are required")
    suffix = sha256(repository.casefold().encode()).hexdigest()[:8]
    return (
        f"fabricqueryr-integration-{suffix}-{lane}",
        f"fabricqueryr-integration; repo={repository.casefold()}; lane={lane}; managed-by={MANAGER}",
    )


def owned_workspace(api, repository, lane, capacity_id, *, create=False):
    name, marker = workspace_identity(repository, lane)
    matches = [w for w in api.list_workspaces() if w.get("displayName") == name]
    if len(matches) > 1:
        raise RuntimeError(f"Ambiguous integration workspace: {name}")
    if not matches:
        if not create:
            return None
        return api.request(
            "POST",
            "/workspaces",
            json={
                "displayName": name,
                "description": marker,
                "capacityId": capacity_id,
            },
        ).json()
    workspace = api.request("GET", f"/workspaces/{matches[0]['id']}").json()
    if workspace.get("description") != marker:
        raise RuntimeError(
            "Refusing integration workspace without exact ownership marker"
        )
    if workspace.get("capacityId", "").casefold() != capacity_id.casefold():
        raise RuntimeError("Integration workspace belongs to a different capacity")
    return workspace


def ensure_targets(api, settings, items, scope):
    """Create missing items only; configuration changes require explicit migration."""
    targets = {}
    configurations = {}

    def ensure(name, kind, route, **body):
        # Reject a conflicting item before creating anything with the same name.
        if any(i.get("displayName") == name and i.get("type") != kind for i in items):
            raise RuntimeError(f"Unexpected item type for {name}")
        item = ensure_item(api, settings.workspace_id, items, name, kind, route, **body)
        targets[name] = item["id"]
        configurations[name] = {"type": kind, **body}
        return item

    ensure(
        "TestLakehouse",
        "Lakehouse",
        "lakehouses",
        creationPayload={"enableSchemas": True},
    )
    ensure(
        "TestLakehouseNoSchemas",
        "Lakehouse",
        "lakehouses",
        creationPayload={"enableSchemas": False},
    )
    ensure(
        "TestWarehouse",
        "Warehouse",
        "warehouses",
        creationPayload={"collationType": "Latin1_General_100_BIN2_UTF8"},
    )
    ensure(
        "TestMirroredDatabase",
        "MirroredDatabase",
        "mirroredDatabases",
        definition={
            "format": "Default",
            "parts": [
                definition_part(
                    "mirroring.json",
                    (
                        settings.repository_root
                        / "infra/fabric/terraform/definitions/open-mirroring.json"
                    )
                    .read_bytes()
                    .replace(b"\r\n", b"\n"),
                )
            ],
        },
    )
    if scope == "all":
        eventhouse = ensure(
            "TestEventhouse",
            "Eventhouse",
            "eventhouses",
            creationPayload={"minimumConsumptionUnits": 0},
        )
        ensure(
            "TestKQLDatabase",
            "KQLDatabase",
            "kqlDatabases",
            creationPayload={
                "databaseType": "ReadWrite",
                "parentEventhouseItemId": eventhouse["id"],
            },
        )
        ensure("TestGraphQL", "GraphQLApi", "graphQLApis")
        if settings.provision_sql_database:
            ensure(
                "TestSQLDatabase",
                "SQLDatabase",
                "sqlDatabases",
                creationPayload={
                    "creationMode": "New",
                    "backupRetentionDays": 1,
                },
            )
    return targets, configurations


def marker_file(service, settings, name):
    return service.get_file_system_client(settings.workspace_id).get_file_client(
        f"{settings.lakehouse_id}/Files/fixtures/fabricqueryr-ci-{name}.json"
    )


def read_marker(service, settings, name):
    try:
        return json.loads(
            marker_file(service, settings, name).download_file().readall()
        )
    except (ResourceNotFoundError, ValueError, UnicodeDecodeError):
        return None


def write_marker(service, settings, name, value):
    marker_file(service, settings, name).upload_data(
        json.dumps(value, sort_keys=True).encode(),
        overwrite=True,
    )


def fixture_is_current(settings, service, scope):
    contract = (
        read_fixture_contract(
            settings.workspace_id,
            settings.lakehouse_id,
            service_client=service,
            scope=scope,
        )
        or {}
    )
    runtime = contract.get("runtime")
    return isinstance(runtime, dict) and (
        runtime.get("lane") == settings.spark_runtime_lane
        and runtime.get("fabric_runtime") == settings.spark_runtime_version
        and contract.get("revision") == fixture_revision(settings, runtime, scope=scope)
    )


def prepare(api, credential, service, settings, repository, *, reseed=False):
    lane = settings.spark_runtime_lane
    scope = "all" if lane == "core" else "onelake"
    workspace = owned_workspace(
        api, repository, lane, settings.capacity_id, create=True
    )
    settings = replace(
        settings, workspace_id=workspace["id"], workspace_name=workspace["displayName"]
    )
    items = api.list_items(settings.workspace_id)
    targets, configurations = ensure_targets(api, settings, items, scope)
    settings = replace(
        settings,
        lakehouse_id=targets["TestLakehouse"],
        non_schema_lakehouse_id=targets["TestLakehouseNoSchemas"],
    )
    try:
        service.get_file_system_client(settings.workspace_id).get_directory_client(
            f"{settings.lakehouse_id}/Files/fixtures"
        ).create_directory()
    except ResourceExistsError:
        pass
    previous = read_marker(service, settings, "targets") or {}
    # Do not silently accept an incompatible existing item or replace its data.
    for name in set(previous.get("ids", {})) & set(targets):
        if (
            previous["ids"][name] == targets[name]
            and previous.get("configurations", {}).get(name) != configurations[name]
        ):
            raise RuntimeError(
                f"Persistent configuration changed for {name}; migrate this item explicitly"
            )
    changed_targets = previous.get("ids") != targets
    needs_seed = (
        reseed or changed_targets or not fixture_is_current(settings, service, scope)
    )
    available = {f"{i['displayName']}.{i['type']}" for i in items}
    if scope == "all" and f"{SEMANTIC_MODEL_NAME}.SemanticModel" not in available:
        needs_seed = True
    selected = deployment_items(settings, scope=scope)
    stale = set(
        stale_deployments(
            settings,
            settings.workspace_id,
            settings.lakehouse_id,
            service_client=service,
            items=selected,
        )
    )
    stale.update(set(selected) - available)
    if needs_seed:
        # Persist invalidation before publishing or seeding; interruption is retryable.
        write_fixture_revision(
            settings.workspace_id,
            settings.lakehouse_id,
            INCOMPLETE_FIXTURE_REVISION,
            service_client=service,
            scope=scope,
        )
        write_marker(service, settings, "snapshot", None)
        stale.add("SeedFixtures.Notebook")
    write_marker(
        service, settings, "targets", {"ids": targets, "configurations": configurations}
    )
    if stale:
        print("Publishing changed definitions: " + ", ".join(sorted(stale)), flush=True)
        deploy(settings, items=sorted(stale))
    if needs_seed:
        print(f"Seeding stale {scope} fixtures", flush=True)
        seed(settings, scope=scope)
    else:
        print(f"Reusing {scope} fixtures; no seed job submitted", flush=True)
    # Refresh a changed import model independently of Spark/SQL/KQL seed data.
    if scope == "all":
        model = f"{ARROW_SEMANTIC_MODEL_NAME}.SemanticModel"
        model_revision = deployment_revision(settings, model)
        if not needs_seed and (
            model in stale or read_marker(service, settings, "model") != model_revision
        ):
            prepare_arrow_test_semantic_model(credential, settings.workspace_id)
        write_marker(service, settings, "model", model_revision)
        snapshot_contract = read_fixture_contract(
            settings.workspace_id,
            settings.lakehouse_id,
            service_client=service,
            scope=scope,
        )
        snapshots = [
            i
            for i in items
            if i["displayName"] == "TestWarehouseSnapshot"
            and i["type"] == "WarehouseSnapshot"
        ]
        if len(snapshots) > 1:
            raise RuntimeError("Ambiguous Warehouse snapshot")
        if read_marker(service, settings, "snapshot") != snapshot_contract:
            for snapshot in snapshots:
                complete_operation(
                    api,
                    api.request(
                        "DELETE",
                        f"/workspaces/{settings.workspace_id}/items/{snapshot['id']}",
                    ),
                    "Delete stale snapshot",
                )
                items.remove(snapshot)
        ensure_item(
            api,
            settings.workspace_id,
            items,
            "TestWarehouseSnapshot",
            "WarehouseSnapshot",
            "warehouseSnapshots",
            creationPayload={"parentWarehouseId": targets["TestWarehouse"]},
        )
        write_marker(service, settings, "snapshot", snapshot_contract)
    (discover if scope == "all" else discover_onelake)(settings)
    print(f"Persistent {lane} manifest: {settings.manifest_path}", flush=True)
    return settings


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "clean"))
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--reseed", action="store_true")
    args = parser.parse_args(argv)
    settings = SandboxSettings.from_environment()
    if not args.repository or not settings.capacity_id:
        parser.error("GITHUB_REPOSITORY and FABRIC_CAPACITY_ID are required")
    workspace_identity(args.repository, settings.spark_runtime_lane)
    credential = CachedTokenCredential(get_credential())
    with (
        FabricApi(credential) as api,
        DataLakeServiceClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            credential=credential,
        ) as service,
    ):
        if args.action == "prepare":
            wait_active_capacity(api, settings.capacity_id)
        workspace = owned_workspace(
            api, args.repository, settings.spark_runtime_lane, settings.capacity_id
        )
        if workspace:
            from .reset_ci import clean_workspace

            clean_workspace(
                api,
                credential,
                service,
                workspace["id"],
                scratch=args.action == "prepare",
            )
        if args.action == "prepare":
            prepare(
                api, credential, service, settings, args.repository, reseed=args.reseed
            )


if __name__ == "__main__":
    main()
