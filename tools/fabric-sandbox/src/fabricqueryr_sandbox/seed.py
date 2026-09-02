"""Upload fixture files and run the deterministic seed notebook."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from datetime import datetime, timezone

from azure.core.credentials import TokenCredential
from azure.core.exceptions import AzureError, ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient

from .credentials import CachedTokenCredential, get_credential, STORAGE_SCOPE
from .discover import (
    _wait_for_kql_properties,
    _wait_for_sql_properties,
)
from .fabric_api import FABRIC_SCOPE, FabricApi
from .fixture_revision import (
    FIXTURE_SCOPES,
    INCOMPLETE_FIXTURE_REVISION,
    fixture_revision,
    write_fixture_revision,
)
from .graphql_api import (
    GRAPHQL_API_NAME,
    GRAPHQL_ROOT_FIELD,
    graphql_definition,
)
from .kusto_api import KUSTO_SCOPE, KustoApi, SEED_TABLE
from .open_mirroring import (
    MIRRORED_FIXTURE_TABLE,
    upload_open_mirroring_fixture,
)
from .power_bi_api import (
    POWER_BI_SCOPE,
    prepare_arrow_test_semantic_model,
    seed_test_semantic_model,
)
from .settings import SandboxSettings
from .sql_api import (
    SQL_AUDIENCE,
    SQL_FIXTURE_TABLE,
    SQL_MUTATION_TABLE,
    seed_sql_fixture,
    wait_for_sql_fixture,
)


RUNTIME_VERIFICATION_ERROR = (
    "fabricqueryr-seed-error: verify and record Fabric Spark runtime"
)
RUNTIME_PROPAGATION_ATTEMPTS = 3
RUNTIME_PROPAGATION_RETRY_SECONDS = 30


def _runtime_contract(
    exit_value: object,
    settings: SandboxSettings,
) -> dict[str, str]:
    prefix = "fabricqueryr-seed-success:"
    if not isinstance(exit_value, str) or not exit_value.startswith(prefix):
        raise RuntimeError("seed notebook did not report its runtime build")
    try:
        value = json.loads(exit_value.removeprefix(prefix))
    except json.JSONDecodeError as error:
        raise RuntimeError("seed notebook returned invalid runtime metadata") from error
    required = {"lane", "fabric_runtime", "spark_version", "delta_version"}
    if (
        not isinstance(value, dict)
        or set(value) != required
        or not all(isinstance(value[key], str) and value[key] for key in required)
        or value["lane"] != settings.spark_runtime_lane
        or value["fabric_runtime"] != settings.spark_runtime_version
    ):
        raise RuntimeError(
            f"seed notebook runtime metadata does not match the lane: {value!r}"
        )
    return value


def _run_seed_notebook(
    api: FabricApi,
    workspace_id: str,
    notebook_id: str,
    lakehouse_id: str,
    *,
    attempts: int = RUNTIME_PROPAGATION_ATTEMPTS,
    retry_delay: float = RUNTIME_PROPAGATION_RETRY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    for attempt in range(1, attempts + 1):
        try:
            return api.run_notebook(
                workspace_id,
                notebook_id,
                lakehouse_id=lakehouse_id,
            )
        except RuntimeError as error:
            runtime_not_ready = str(error).startswith(
                RUNTIME_VERIFICATION_ERROR
            )
            if not runtime_not_ready or attempt == attempts:
                raise
            print(
                "Fabric Spark runtime has not propagated to notebook "
                f"sessions; retrying seed ({attempt + 1}/{attempts})"
            )
            sleep(retry_delay)
    raise AssertionError("unreachable")


def _logical_delta_table_row_count(delta_table: object) -> int:
    """Count active rows, excluding rows masked by deletion vectors."""
    physical_rows = delta_table.count()
    deletion_vectors = delta_table.deletion_vectors().read_all()
    deleted_rows = sum(
        selection_vector.count(False)
        for selection_vector in deletion_vectors.column(
            "selection_vector"
        ).to_pylist()
    )
    return physical_rows - deleted_rows


def _utc_datetime(value: datetime) -> datetime:
    """Interpret timezone-free Fabric storage timestamps as UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def wait_for_delta_log_publication(
    workspace_id: str,
    item_id: str,
    table: str,
    *,
    not_before: datetime,
    attempts: int = 60,
    retry_delay: float = 10,
    expected_rows: int | None = None,
    service_client: DataLakeServiceClient | None = None,
    table_count: Callable[[str], int] | None = None,
    credential: TokenCredential | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Wait for Fabric's background OneLake Delta-log publication."""
    if attempts < 1:
        raise ValueError("attempts must be positive")
    if not_before.tzinfo is None:
        raise ValueError("not_before must be timezone-aware")
    if service_client is None:
        credential = credential or get_credential()
        service = DataLakeServiceClient(
            account_url="https://onelake.dfs.fabric.microsoft.com",
            credential=credential,
        )
    else:
        service = service_client
    filesystem = service.get_file_system_client(workspace_id)
    log_path = f"{item_id}/Tables/dbo/{table}/_delta_log"
    table_uri = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{item_id}/Tables/dbo/{table}"
    )
    if expected_rows is not None and table_count is None:
        from deltalake import DeltaTable

        credential = credential or get_credential()
        storage_token = credential.get_token(STORAGE_SCOPE).token

        def table_count(uri: str) -> int:
            delta_table = DeltaTable(
                uri,
                storage_options={
                    "bearer_token": storage_token,
                    "use_fabric_endpoint": "true",
                },
            )
            return _logical_delta_table_row_count(delta_table)

    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            paths = filesystem.get_paths(path=log_path, recursive=False)
            for path in paths:
                name = getattr(path, "name", "")
                modified = getattr(path, "last_modified", None)
                if (
                    not getattr(path, "is_directory", False)
                    and name.endswith((".json", ".parquet"))
                    and modified is not None
                    and _utc_datetime(modified) >= _utc_datetime(not_before)
                ):
                    if expected_rows is None:
                        return name
                    try:
                        actual_rows = table_count(table_uri)
                    except Exception as error:
                        last_error = error
                        continue
                    if actual_rows == expected_rows:
                        return name
                    last_error = RuntimeError(
                        f"Warehouse Delta table has {actual_rows} rows; "
                        f"expected {expected_rows}: {table_uri}"
                    )
        except AzureError as error:
            last_error = error
        if attempt < attempts:
            sleep(retry_delay)
    raise RuntimeError(
        "OneLake Delta log was not published and readable after "
        f"{attempts} attempts: {log_path}"
    ) from last_error


def upload_fixtures(
    settings: SandboxSettings,
    workspace_id: str,
    lakehouse_id: str,
    *,
    credential: TokenCredential | None = None,
) -> None:
    service = DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=credential or get_credential(),
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


def seed(settings: SandboxSettings, *, scope: str = "all") -> None:
    """Seed all fixtures or the fixtures needed by one test scope."""
    if scope not in FIXTURE_SCOPES:
        raise ValueError(
            f"Unknown fixture scope {scope!r}; expected one of "
            + ", ".join(FIXTURE_SCOPES)
        )
    workspace_id = settings.require_workspace()
    credential = CachedTokenCredential(get_credential())
    token_scopes = (FABRIC_SCOPE, STORAGE_SCOPE)
    if scope != "jobs":
        token_scopes += (SQL_AUDIENCE,)
    if scope == "all":
        token_scopes += (KUSTO_SCOPE, POWER_BI_SCOPE)
    for token_scope in token_scopes:
        credential.get_token(token_scope)

    with FabricApi(credential) as api:
        spark_settings = api.configure_workspace_spark_runtime(
            workspace_id,
            settings.spark_runtime_version,
        )
        actual_runtime = spark_settings.get("environment", {}).get(
            "runtimeVersion"
        )
        if actual_runtime != settings.spark_runtime_version:
            raise RuntimeError(
                "Fabric did not apply the requested Spark runtime "
                f"{settings.spark_runtime_version!r}; got {actual_runtime!r}"
            )
        lakehouse = api.find_item(workspace_id, "TestLakehouse", "Lakehouse")
        notebook = api.find_item(workspace_id, "SeedFixtures", "Notebook")
        incomplete_scopes = (
            ("all", "onelake") if scope == "all" else (scope,)
        )
        for marker_scope in incomplete_scopes:
            write_fixture_revision(
                workspace_id,
                lakehouse["id"],
                INCOMPLETE_FIXTURE_REVISION,
                credential=credential,
                scope=marker_scope,
            )
        if scope != "jobs":
            warehouse_item = api.find_item(
                workspace_id,
                "TestWarehouse",
                "Warehouse",
            )
            mirrored_database_item = api.find_item(
                workspace_id,
                "TestMirroredDatabase",
                "MirroredDatabase",
            )
            warehouse = _wait_for_sql_properties(
                api,
                workspace_id,
                warehouse_item["id"],
                item_type="Warehouse",
            )
            mirrored_database = _wait_for_sql_properties(
                api,
                workspace_id,
                mirrored_database_item["id"],
                item_type="MirroredDatabase",
            )
            api.wait_for_mirroring_running(
                workspace_id,
                mirrored_database_item["id"],
            )
            mirrored_publication_start = datetime.now(timezone.utc)
            mirrored_remote_path = upload_open_mirroring_fixture(
                workspace_id,
                mirrored_database_item["id"],
                credential=credential,
            )
            print(f"Open mirroring fixture uploaded: {mirrored_remote_path}")
        if scope == "all":
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
            if settings.provision_sql_database:
                sql_database_item = api.find_item(
                    workspace_id,
                    "TestSQLDatabase",
                    "SQLDatabase",
                )
                sql_database = _wait_for_sql_properties(
                    api,
                    workspace_id,
                    sql_database_item["id"],
                    item_type="SQLDatabase",
                )
        upload_fixtures(
            settings,
            workspace_id,
            lakehouse["id"],
            credential=credential,
        )
        job = _run_seed_notebook(
            api,
            workspace_id,
            notebook["id"],
            lakehouse["id"],
        )
        print(
            f"seed notebook completed: {job.get('id')} "
            f"exitValue={job.get('exitValue')!r}"
        )
        runtime_contract = _runtime_contract(job.get("exitValue"), settings)
    if scope == "jobs":
        revision = fixture_revision(
            settings,
            runtime_contract,
            scope=scope,
        )
        write_fixture_revision(
            workspace_id,
            lakehouse["id"],
            revision,
            runtime_contract=runtime_contract,
            credential=credential,
            scope=scope,
        )
        print(f"Fabric jobs fixture revision published: {revision}")
        return
    sql_targets = [
        (
            warehouse_item["displayName"],
            warehouse["properties"]["connectionString"],
            warehouse_item["displayName"],
        ),
    ]
    if scope == "all" and settings.provision_sql_database:
        sql_targets.append(
            (
                sql_database_item["displayName"],
                sql_database["properties"]["connectionString"],
                sql_database["properties"]["databaseName"],
            )
        )
    for display_name, connection_string, database_name in sql_targets:
        sql_token = credential.get_token(SQL_AUDIENCE).token
        publication_start = datetime.now(timezone.utc)
        seed_sql_fixture(
            connection_string,
            database_name,
            sql_token,
            mutate=display_name == warehouse_item["displayName"],
        )
        print(f"SQL fixture seeded: {display_name}.dbo.fabricqueryr_sql_types")
        if display_name == warehouse_item["displayName"]:
            for table in (SQL_FIXTURE_TABLE, SQL_MUTATION_TABLE):
                published = wait_for_delta_log_publication(
                    workspace_id,
                    warehouse_item["id"],
                    table,
                    not_before=publication_start,
                    expected_rows=3,
                    credential=credential,
                )
                print(
                    "Warehouse Delta log published and readable: "
                    f"{published}"
                )

    mirrored_published = wait_for_delta_log_publication(
        workspace_id,
        mirrored_database_item["id"],
        MIRRORED_FIXTURE_TABLE,
        not_before=mirrored_publication_start,
        expected_rows=3,
        credential=credential,
    )
    print(
        "Mirrored Database Delta log published and readable: "
        f"{mirrored_published}"
    )
    mirrored_sql = mirrored_database["properties"]["sqlEndpointProperties"]
    sql_token = credential.get_token(SQL_AUDIENCE).token
    wait_for_sql_fixture(
        mirrored_sql["connectionString"],
        mirrored_database_item["displayName"],
        sql_token,
        MIRRORED_FIXTURE_TABLE,
    )

    onelake_revision = fixture_revision(
        settings,
        runtime_contract,
        scope="onelake",
    )
    write_fixture_revision(
        workspace_id,
        lakehouse["id"],
        onelake_revision,
        runtime_contract=runtime_contract,
        credential=credential,
        scope="onelake",
    )
    print(f"Fabric OneLake fixture revision published: {onelake_revision}")
    if scope == "onelake":
        return

    with FabricApi(credential) as api:
        api.update_graphql_definition(
            workspace_id,
            graphql_api["id"],
            graphql_definition(workspace_id, warehouse_item["id"]),
        )
        ready_field = api.wait_for_graphql_root_field(
            workspace_id,
            graphql_api["id"],
            GRAPHQL_ROOT_FIELD,
        )
        print(
            "GraphQL fixture ready: "
            f"{graphql_api['displayName']}.{ready_field['name']}"
        )

    query_service_uri = kql_database.get("properties", {}).get("queryServiceUri")
    if not query_service_uri:
        raise RuntimeError("KQL database query service URI is not ready")
    with KustoApi(credential) as kusto:
        kusto.seed_fixture(
            query_service_uri,
            kql_database_item["displayName"],
        )
    print(
        "KQL fixture seeded: "
        f"{kql_database_item['displayName']}.{SEED_TABLE}"
    )

    semantic_model = seed_test_semantic_model(
        credential,
        workspace_id,
    )
    print(
        "semantic model seeded: "
        f"{semantic_model.get('name')} ({semantic_model.get('id')})"
    )
    arrow_semantic_model = prepare_arrow_test_semantic_model(
        credential,
        workspace_id,
    )
    print(
        "Arrow semantic model refreshed: "
        f"{arrow_semantic_model.get('name')} "
        f"({arrow_semantic_model.get('id')})"
    )
    revision = fixture_revision(settings, runtime_contract, scope="all")
    write_fixture_revision(
        workspace_id,
        lakehouse["id"],
        revision,
        runtime_contract=runtime_contract,
        credential=credential,
        scope="all",
    )
    print(f"Fabric fixture revision published: {revision}")
