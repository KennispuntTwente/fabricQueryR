"""Upload fixture files and run the deterministic seed notebook."""

from azure.storage.filedatalake import DataLakeServiceClient

from .credentials import get_credential
from .discover import _wait_for_kql_properties
from .fabric_api import FabricApi
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
    for local_path in settings.fixture_dir.rglob("*"):
        if not local_path.is_file():
            continue
        relative_path = local_path.relative_to(settings.fixture_dir).as_posix()
        remote_path = f"{lakehouse_id}/Files/fixtures/{relative_path}"
        with local_path.open("rb") as source:
            filesystem.get_file_client(remote_path).upload_data(source, overwrite=True)


def seed(settings: SandboxSettings) -> None:
    workspace_id = settings.require_workspace()
    with FabricApi(get_credential()) as api:
        lakehouse = api.find_item(workspace_id, "TestLakehouse", "Lakehouse")
        notebook = api.find_item(workspace_id, "SeedFixtures", "Notebook")
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
