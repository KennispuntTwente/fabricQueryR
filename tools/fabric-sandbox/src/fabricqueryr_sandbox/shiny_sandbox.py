"""Provision only the persistent data fixtures used by playground/shiny."""

from __future__ import annotations

import argparse
import base64
import io
import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient

from .cleanup import parse_persistent_description
from .credentials import CachedTokenCredential, get_credential
from .discover import _wait_for_kql_properties, _wait_for_sql_properties
from .fabric_api import FabricApi
from .graphql_api import GRAPHQL_ROOT_FIELD, graphql_definition
from .kusto_api import KustoApi
from .open_mirroring import MIRRORED_FIXTURE_TABLE, upload_open_mirroring_fixture
from .power_bi_api import (
    ARROW_SEMANTIC_MODEL_NAME,
    SEMANTIC_MODEL_NAME,
    prepare_arrow_test_semantic_model,
    seed_test_semantic_model,
)
from .seed import wait_for_delta_log_publication
from .sql_api import SQL_AUDIENCE, seed_sql_fixture, wait_for_sql_fixture

WORKSPACE_NAME = "fabricqueryr-shiny-dhrkoning"
MANAGER = ".github/workflows/fabric-sandbox.yaml"
FIXTURE_VERSION = "shiny-v1"
REQUIRED_ITEMS = {
    "TestLakehouse": "Lakehouse",
    "TestWarehouse": "Warehouse",
    "TestMirroredDatabase": "MirroredDatabase",
    "TestEventhouse": "Eventhouse",
    "TestKQLDatabase": "KQLDatabase",
    "TestGraphQL": "GraphQLApi",
    SEMANTIC_MODEL_NAME: "SemanticModel",
    ARROW_SEMANTIC_MODEL_NAME: "SemanticModel",
}


def wait_active_capacity(api, capacity_id, *, attempts=60):
    for _ in range(attempts):
        path = "/capacities"
        capacities = []
        while path:
            page = api.request("GET", path).json()
            capacities.extend(page.get("value", []))
            path = page.get("continuationUri")
        matches = [
            item
            for item in capacities
            if item["id"].casefold() == capacity_id.casefold()
        ]
        if len(matches) != 1 or matches[0].get("sku") != "F2":
            raise RuntimeError(
                "The Shiny sandbox requires the configured, accessible F2 capacity"
            )
        if matches[0].get("state") == "Active":
            return
        api.sleep(10)
    raise TimeoutError("Fabric did not report the F2 capacity as active")


def workspace_marker(repository, owner, *, ready=False):
    timestamp = datetime.now(timezone.utc).isoformat()
    run = os.environ.get("GITHUB_RUN_ID", "local")
    return (
        f"fabricqueryr-persistent; repo={repository}; owner={owner}; "
        f"managed-by={MANAGER}; rebuilt={timestamp}; run={run}; "
        f"fixture={FIXTURE_VERSION}; ready={'true' if ready else 'false'}"
    )


def ensure_workspace(api, capacity_id, repository, owner):
    matches = [
        workspace
        for workspace in api.list_workspaces()
        if workspace.get("displayName") == WORKSPACE_NAME
    ]
    if len(matches) > 1:
        raise RuntimeError("The Shiny workspace name is ambiguous")
    if matches:
        workspace = api.request("GET", f"/workspaces/{matches[0]['id']}").json()
        marker = parse_persistent_description(workspace.get("description"))
        if not marker or any(
            marker[key].casefold() != value.casefold()
            for key, value in {
                "repo": repository,
                "owner": owner,
                "managed-by": MANAGER,
            }.items()
        ):
            raise RuntimeError(
                "Refusing to modify a workspace without matching Shiny ownership"
            )
        if workspace.get("capacityId", "").casefold() != capacity_id.casefold():
            raise RuntimeError(
                "The Shiny workspace is not assigned to the configured F2"
            )
        return workspace
    return api.request(
        "POST",
        "/workspaces",
        json={
            "displayName": WORKSPACE_NAME,
            "capacityId": capacity_id,
            "description": workspace_marker(repository, owner),
        },
    ).json()


def ensure_owner(api, workspace_id, owner):
    path = f"/workspaces/{workspace_id}/roleAssignments"
    assignments = []
    while path:
        page = api.request("GET", path).json()
        assignments.extend(page.get("value", []))
        path = page.get("continuationUri")
    matches = [
        entry
        for entry in assignments
        if entry.get("principal", {}).get("id", "").casefold() == owner.casefold()
    ]
    if len(matches) > 1:
        raise RuntimeError("Multiple workspace assignments match the sandbox owner")
    if not matches:
        api.request(
            "POST",
            f"/workspaces/{workspace_id}/roleAssignments",
            json={
                "principal": {"id": owner, "type": "User"},
                "role": "Admin",
            },
        )
    elif matches[0].get("role") != "Admin":
        api.request(
            "PATCH",
            f"/workspaces/{workspace_id}/roleAssignments/{matches[0]['id']}",
            json={"role": "Admin"},
        )


def complete_operation(api, response, name, *, result=False):
    if response.status_code == 202:
        return api._wait_for_operation(
            response, operation_name=name, timeout=1200, return_result=result
        )
    return response.json() if response.content else {}


def ensure_item(api, workspace_id, items, name, kind, route, **body):
    matches = [
        item
        for item in items
        if item.get("displayName") == name and item.get("type") == kind
    ]
    if len(matches) > 1:
        raise RuntimeError(f"Ambiguous Shiny fixture: {name} ({kind})")
    if matches:
        return matches[0]
    print(f"Creating {name} ({kind})", flush=True)
    response = api.request(
        "POST",
        f"/workspaces/{workspace_id}/{route}",
        json={"displayName": name, **body},
    )
    item = complete_operation(api, response, f"Create {name}", result=True)
    items.append(item)
    return item


def definition_part(path, content):
    return {
        "path": path,
        "payloadType": "InlineBase64",
        "payload": base64.b64encode(content).decode("ascii"),
    }


def ensure_targets(api, workspace_id, items, root):
    lakehouse = ensure_item(
        api,
        workspace_id,
        items,
        "TestLakehouse",
        "Lakehouse",
        "lakehouses",
        creationPayload={"enableSchemas": True},
    )
    warehouse = ensure_item(
        api, workspace_id, items, "TestWarehouse", "Warehouse", "warehouses"
    )
    mirrored = ensure_item(
        api,
        workspace_id,
        items,
        "TestMirroredDatabase",
        "MirroredDatabase",
        "mirroredDatabases",
        definition={
            "format": "Default",
            "parts": [
                definition_part(
                    "mirroring.json",
                    (
                        root / "infra/fabric/terraform/definitions/open-mirroring.json"
                    ).read_bytes(),
                )
            ],
        },
    )
    eventhouse = ensure_item(
        api,
        workspace_id,
        items,
        "TestEventhouse",
        "Eventhouse",
        "eventhouses",
        creationPayload={"minimumConsumptionUnits": 0},
    )
    kql = ensure_item(
        api,
        workspace_id,
        items,
        "TestKQLDatabase",
        "KQLDatabase",
        "kqlDatabases",
        creationPayload={
            "databaseType": "ReadWrite",
            "parentEventhouseItemId": eventhouse["id"],
        },
    )
    graphql = ensure_item(
        api, workspace_id, items, "TestGraphQL", "GraphQLApi", "graphQLApis"
    )
    model_dir = (
        root / "infra/fabric/workspace" / f"{ARROW_SEMANTIC_MODEL_NAME}.SemanticModel"
    )
    ensure_item(
        api,
        workspace_id,
        items,
        ARROW_SEMANTIC_MODEL_NAME,
        "SemanticModel",
        "semanticModels",
        definition={
            "format": "TMSL",
            "parts": [
                definition_part(name, (model_dir / name).read_bytes())
                for name in ("model.bim", "definition.pbism")
            ],
        },
    )
    return {
        "lakehouse": lakehouse,
        "warehouse": warehouse,
        "mirrored": mirrored,
        "kql": kql,
        "graphql": graphql,
    }


def lakehouse_fixture_files(root):
    import pyarrow as pa
    from pyarrow import csv, parquet

    basic_csv = (root / "infra/fabric/fixtures/basic.csv").read_bytes()
    table = csv.read_csv(
        io.BytesIO(basic_csv),
        convert_options=csv.ConvertOptions(
            column_types={
                "id": pa.int32(),
                "name": pa.string(),
                "category": pa.string(),
                "amount": pa.float64(),
            }
        ),
    )
    buffer = io.BytesIO()
    parquet.write_table(table, buffer)
    return {"basic.csv": basic_csv, "basic.parquet": buffer.getvalue()}


def seed_lakehouse(api, credential, workspace_id, lakehouse_id, root):
    with DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com", credential=credential
    ) as service:
        filesystem = service.get_file_system_client(workspace_id)
        directory = f"{lakehouse_id}/Files/fixtures"
        try:
            filesystem.get_directory_client(directory).create_directory()
        except ResourceExistsError:
            pass
        for name, content in lakehouse_fixture_files(root).items():
            filesystem.get_file_client(f"{directory}/{name}").upload_data(
                content, overwrite=True
            )
    # The load API creates the Delta table; no test notebook or Spark job is deployed.
    response = api.request(
        "POST",
        f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}"
        "/schemas/dbo/tables/fabricqueryr_basic/load",
        params={"beta": "true"},
        json={
            "relativePath": "Files/fixtures/basic.parquet",
            "pathType": "File",
            "mode": "Overwrite",
            "formatOptions": {"format": "Parquet"},
        },
    )
    complete_operation(api, response, "Load Shiny Lakehouse fixture")
    for _ in range(90):
        properties = api.get_lakehouse(workspace_id, lakehouse_id)
        endpoint = properties.get("properties", {}).get("sqlEndpointProperties", {})
        if (
            endpoint.get("id")
            and endpoint.get("connectionString")
            and endpoint.get("provisioningStatus") == "Success"
        ):
            break
        api.sleep(10)
    else:
        raise TimeoutError("Lakehouse SQL endpoint was not ready")
    api.refresh_sql_endpoint_metadata(workspace_id, endpoint["id"])
    wait_for_sql_fixture(
        endpoint["connectionString"],
        "TestLakehouse",
        credential.get_token(SQL_AUDIENCE).token,
        "fabricqueryr_basic",
    )


def seed_targets(api, credential, workspace_id, targets, root):
    seed_lakehouse(api, credential, workspace_id, targets["lakehouse"]["id"], root)
    warehouse = targets["warehouse"]
    sql = _wait_for_sql_properties(
        api, workspace_id, warehouse["id"], item_type="Warehouse"
    )
    seed_sql_fixture(
        sql["properties"]["connectionString"],
        warehouse["displayName"],
        credential.get_token(SQL_AUDIENCE).token,
        mutate=True,
    )
    print("Warehouse rows verified", flush=True)

    mirrored_id = targets["mirrored"]["id"]
    api.wait_for_mirroring_running(workspace_id, mirrored_id)
    started = datetime.now(timezone.utc)
    upload_open_mirroring_fixture(workspace_id, mirrored_id, credential=credential)
    wait_for_delta_log_publication(
        workspace_id,
        mirrored_id,
        MIRRORED_FIXTURE_TABLE,
        not_before=started,
        expected_rows=3,
        credential=credential,
    )
    print("Mirrored Delta rows verified", flush=True)

    kql = targets["kql"]
    properties = _wait_for_kql_properties(
        api, workspace_id, kql["id"], item_type="KQLDatabase"
    )
    with KustoApi(credential) as kusto:
        kusto.seed_fixture(
            properties["properties"]["queryServiceUri"], kql["displayName"]
        )
    api.update_graphql_definition(
        workspace_id,
        targets["graphql"]["id"],
        graphql_definition(workspace_id, warehouse["id"]),
    )
    api.wait_for_graphql_root_field(
        workspace_id, targets["graphql"]["id"], GRAPHQL_ROOT_FIELD
    )
    seed_test_semantic_model(credential, workspace_id)
    prepare_arrow_test_semantic_model(credential, workspace_id)
    print("KQL, GraphQL and semantic-model fixtures ready", flush=True)


def prepare(api, credential, capacity_id, repository, owner, root, *, reseed=False):
    wait_active_capacity(api, capacity_id)
    workspace = ensure_workspace(api, capacity_id, repository, owner)
    workspace_id = workspace["id"]
    ensure_owner(api, workspace_id, owner)
    items = api.list_items(workspace_id)
    available = {(item.get("displayName"), item.get("type")) for item in items}
    marker = parse_persistent_description(workspace.get("description")) or {}
    ready = marker.get("ready") == "true" and marker.get("fixture") == FIXTURE_VERSION
    if ready and not reseed and set(REQUIRED_ITEMS.items()).issubset(available):
        print("Reusing the existing Shiny fixtures; no reseed requested", flush=True)
        return workspace_id
    api.request(
        "PATCH",
        f"/workspaces/{workspace_id}",
        json={"description": workspace_marker(repository, owner)},
    )
    targets = ensure_targets(api, workspace_id, items, root)
    seed_targets(api, credential, workspace_id, targets, root)
    api.request(
        "PATCH",
        f"/workspaces/{workspace_id}",
        json={"description": workspace_marker(repository, owner, ready=True)},
    )
    return workspace_id


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
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
    root = Path(__file__).resolve().parents[4]
    credential = CachedTokenCredential(get_credential())
    with FabricApi(credential) as api:
        workspace_id = prepare(
            api,
            credential,
            args.capacity_id,
            args.repository,
            args.owner_id,
            root,
            reseed=args.reseed,
        )
    print(f"Shiny sandbox ready: {WORKSPACE_NAME}")
    if summary := os.environ.get("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a", encoding="utf-8") as stream:
            stream.write(
                f"## Shiny sandbox ready\n\nWorkspace: `{WORKSPACE_NAME}`\n\n"
                f"[Open workspace](https://app.fabric.microsoft.com/groups/{workspace_id}/list)\n\n"
                "```r\nSys.setenv(FABRIC_SHINY_WORKSPACE = 'fabricqueryr-shiny-dhrkoning')\n"
                "shiny::runApp('playground/shiny', port = 8100)\n```\n\n"
                "The F2 remains active. Run this workflow with `shiny-pause` when finished.\n"
            )


if __name__ == "__main__":
    main()
