"""Upload fixture files and run the deterministic seed notebook."""

from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient

from .credentials import get_credential
from .discover import (
    _wait_for_kql_properties,
    _wait_for_lakehouse_sql_endpoint,
)
from .fabric_api import FabricApi
from .graphql_api import (
    GRAPHQL_API_NAME,
    GRAPHQL_SOURCE_OBJECT,
    GRAPHQL_TYPE,
    graphql_definition,
)
from .kusto_api import KustoApi, SEED_TABLE
from .power_bi_api import seed_test_semantic_model
from .settings import SandboxSettings


def upload_fixtures(
    settings: SandboxSettings, workspace_id: str, lakehouse_id: str
) -> None:
    service = DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=get_credential(),
    )
    filesystem = service.get_file_system_client(workspace_id)
    fixture_files = sorted(
        path for path in settings.fixture_dir.rglob("*") if path.is_file()
    )
    directories = {f"{lakehouse_id}/Files/fixtures"}
    for local_path in fixture_files:
        relative = local_path.relative_to(settings.fixture_dir)
        parent = relative.parent
        while parent != parent.parent and parent.as_posix() != ".":
            directories.add(
                f"{lakehouse_id}/Files/fixtures/{parent.as_posix()}"
            )
            parent = parent.parent
    for directory in sorted(directories, key=lambda value: value.count("/")):
        try:
            filesystem.get_directory_client(directory).create_directory()
        except ResourceExistsError:
            pass

    for local_path in fixture_files:
        relative_path = local_path.relative_to(settings.fixture_dir).as_posix()
        remote_path = f"{lakehouse_id}/Files/fixtures/{relative_path}"
        with local_path.open("rb") as source:
            filesystem.get_file_client(remote_path).upload_data(source, overwrite=True)


def seed(settings: SandboxSettings) -> None:
    workspace_id = settings.require_workspace()
    with FabricApi(get_credential()) as api:
        lakehouse = api.find_item(workspace_id, "TestLakehouse", "Lakehouse")
        notebook = api.find_item(workspace_id, "SeedFixtures", "Notebook")
        graphql_api = api.find_item(
            workspace_id,
            GRAPHQL_API_NAME,
            "GraphQLApi",
        )
        kql_database_item = api.find_item(
            workspace_id,
            "TestKQLDatabase",
            "KQLDatabase",
        )
        kql_database = _wait_for_kql_properties(
            api,
            workspace_id,
            kql_database_item["id"],
            item_type="KQLDatabase",
        )
        upload_fixtures(settings, workspace_id, lakehouse["id"])
        job = api.run_notebook(
            workspace_id,
            notebook["id"],
            lakehouse_id=lakehouse["id"],
        )
        print(
            f"seed notebook completed: {job.get('id')} "
            f"exitValue={job.get('exitValue')!r}"
        )
        lakehouse_properties = _wait_for_lakehouse_sql_endpoint(
            api,
            workspace_id,
            lakehouse["id"],
        )["properties"]
        sql_endpoint_id = lakehouse_properties["sqlEndpointProperties"]["id"]
        sync_status = api.wait_for_sql_endpoint_table(
            workspace_id,
            sql_endpoint_id,
            GRAPHQL_SOURCE_OBJECT,
        )
        print(
            "SQL endpoint fixture ready: "
            f"{sync_status.get('tableName')} "
            f"status={sync_status.get('status')!r}"
        )
        api.update_graphql_definition(
            workspace_id,
            graphql_api["id"],
            graphql_definition(workspace_id, sql_endpoint_id),
        )
        ready_type = api.wait_for_graphql_type(
            workspace_id,
            graphql_api["id"],
            GRAPHQL_TYPE,
        )
        print(
            "GraphQL fixture ready: "
            f"{graphql_api['displayName']}.{ready_type['name']}"
        )

    query_service_uri = kql_database.get("properties", {}).get("queryServiceUri")
    if not query_service_uri:
        raise RuntimeError("KQL database query service URI is not ready")
    with KustoApi(get_credential()) as kusto:
        kusto.seed_fixture(
            query_service_uri,
            kql_database_item["displayName"],
        )
    print(
        "KQL fixture seeded: "
        f"{kql_database_item['displayName']}.{SEED_TABLE}"
    )

    semantic_model = seed_test_semantic_model(
        get_credential(),
        workspace_id,
    )
    print(
        "semantic model seeded: "
        f"{semantic_model.get('name')} ({semantic_model.get('id')})"
    )
