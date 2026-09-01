"""Discover deployed endpoints and write the R test manifest."""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import quote

from .credentials import get_credential
from .fabric_api import FabricApi
from .fixture_revision import read_fixture_contract, verify_fixture_revision
from .graphql_api import (
    GRAPHQL_API_NAME,
    GRAPHQL_CREATE_FIELD,
    GRAPHQL_ROOT_FIELD,
)
from .manifest import SandboxManifest
from .open_mirroring import MIRRORED_FIXTURE_SCHEMA, MIRRORED_FIXTURE_TABLE
from .power_bi_api import (
    ARROW_SEMANTIC_MODEL_NAME,
    PowerBiApi,
    SEMANTIC_MODEL_NAME,
)
from .settings import SandboxSettings
from .sql_api import SQL_FIXTURE_TABLE, SQL_FIXTURE_VIEW, SQL_MUTATION_TABLE


ONELAKE_LAKEHOUSE_TABLES = {
    "runtime": "fabricqueryr_runtime",
    "basic": "fabricqueryr_basic",
    "non_default_basic": "fabricqueryr_basic",
    "empty": "fabricqueryr_empty",
    "void": "fabricqueryr_void",
    "partitioned": "fabricqueryr_partitioned",
    "typed_partitions": "fabricqueryr_typed_partitions",
    "binary_partitions": "fabricqueryr_binary_partitions",
    "schema_evolved": "fabricqueryr_schema_evolved",
    "column_mapped": "fabricqueryr_column_mapped",
    "column_mapped_id": "fabricqueryr_column_mapped_id",
    "column_mapped_id_partitioned_dv": (
        "fabricqueryr_column_mapped_id_partitioned_dv"
    ),
    "struct_validity": "fabricqueryr_struct_validity",
    "deletion_vectors": "fabricqueryr_deletion_vectors",
    "file_row_number_collision": "fabricqueryr_file_row_number_collision",
    "deletion_vectors_stress": "fabricqueryr_deletion_vectors_stress",
    "deletion_vectors_checkpoint": "fabricqueryr_deletion_vectors_checkpoint",
    "deletion_vectors_dense": "fabricqueryr_deletion_vectors_dense",
    "row_tracking": "fabricqueryr_row_tracking",
    "exact_types": "fabricqueryr_exact_types",
    "complex_types": "fabricqueryr_complex_types",
    "oracle_basic": "fabricqueryr_oracle_basic",
    "oracle_empty": "fabricqueryr_oracle_empty",
    "oracle_typed_partitions": "fabricqueryr_oracle_typed_partitions",
    "oracle_partitioned": "fabricqueryr_oracle_partitioned",
    "oracle_schema_evolved": "fabricqueryr_oracle_schema_evolved",
    "oracle_exact_types": "fabricqueryr_oracle_exact_types",
    "oracle_complex_types": "fabricqueryr_oracle_complex_types",
    "spark_oracle_column_mapped": "fabricqueryr_spark_oracle_column_mapped",
    "spark_oracle_column_mapped_id": "fabricqueryr_spark_oracle_column_mapped_id",
    "spark_oracle_column_mapped_id_partitioned_dv": (
        "fabricqueryr_spark_oracle_column_mapped_id_partitioned_dv"
    ),
    "spark_oracle_struct_validity": "fabricqueryr_spark_oracle_struct_validity",
    "spark_oracle_deletion_vectors": "fabricqueryr_spark_oracle_deletion_vectors",
    "spark_oracle_file_row_number_collision": (
        "fabricqueryr_spark_oracle_file_row_number_collision"
    ),
    "spark_oracle_deletion_vectors_stress": (
        "fabricqueryr_spark_oracle_deletion_vectors_stress"
    ),
    "spark_oracle_deletion_vectors_checkpoint": (
        "fabricqueryr_spark_oracle_deletion_vectors_checkpoint"
    ),
    "spark_oracle_deletion_vectors_dense": (
        "fabricqueryr_spark_oracle_deletion_vectors_dense"
    ),
    "spark_oracle_row_tracking": "fabricqueryr_spark_oracle_row_tracking",
    "spark_oracle_type_widened": "fabricqueryr_spark_oracle_type_widened",
    "spark_oracle_type_widened_exact": (
        "fabricqueryr_spark_oracle_type_widened_exact"
    ),
    "spark_oracle_type_widened_pending": (
        "fabricqueryr_spark_oracle_type_widened_pending"
    ),
    "spark_oracle_type_widened_nested": (
        "fabricqueryr_spark_oracle_type_widened_nested"
    ),
    "spark_oracle_type_widened_map_key": (
        "fabricqueryr_spark_oracle_type_widened_map_key"
    ),
    "spark_oracle_v2_checkpoint": "fabricqueryr_spark_oracle_v2_checkpoint",
    "spark_oracle_shallow_clone": "fabricqueryr_spark_oracle_shallow_clone",
    "spark_oracle_variant": "fabricqueryr_spark_oracle_variant",
    "spark_oracle_variant_id_dv": "fabricqueryr_spark_oracle_variant_id_dv",
    "shallow_clone": "fabricqueryr_shallow_clone",
    "type_widened": "fabricqueryr_type_widened",
    "type_widened_exact": "fabricqueryr_type_widened_exact",
    "type_widened_pending": "fabricqueryr_type_widened_pending",
    "type_widened_nested": "fabricqueryr_type_widened_nested",
    "type_widened_map_key": "fabricqueryr_type_widened_map_key",
    "v2_checkpoint": "fabricqueryr_v2_checkpoint",
    "variant": "fabricqueryr_variant",
    "variant_id_dv": "fabricqueryr_variant_id_dv",
    "livy_batch_result": "fabricqueryr_livy_batch_result",
    "spark_job_result": "fabricqueryr_spark_job_result",
}
NON_SCHEMA_LAKEHOUSE_TABLES = {"basic": "fabricqueryr_basic"}
JOBS_LAKEHOUSE_TABLES = {
    "basic": "fabricqueryr_basic",
    "spark_job_result": "fabricqueryr_spark_job_result",
}


def discover_jobs(settings: SandboxSettings) -> SandboxManifest:
    """Write the minimal manifest needed by Fabric item job tests."""
    workspace_id = settings.require_workspace()
    with FabricApi(get_credential()) as api:
        lakehouse_item = api.find_item(workspace_id, "TestLakehouse", "Lakehouse")
        job_notebook_item = api.find_item(
            workspace_id, "JobFixtures", "Notebook"
        )
        pipeline_item = api.find_item(
            workspace_id, "TestPipeline", "DataPipeline"
        )
        spark_job_item = api.find_item(
            workspace_id, "TestSparkJob", "SparkJobDefinition"
        )

    revision = verify_fixture_revision(
        settings,
        workspace_id,
        lakehouse_item["id"],
        scope="jobs",
    )
    fixture_contract = read_fixture_contract(
        workspace_id,
        lakehouse_item["id"],
        scope="jobs",
    )
    manifest = SandboxManifest(
        workspace_id=workspace_id,
        workspace_name=settings.workspace_name,
        fixture_revision=revision,
        runtime=fixture_contract["runtime"],
        items={
            "TestLakehouse": {
                "id": lakehouse_item["id"],
                "type": "Lakehouse",
                "display_name": lakehouse_item["displayName"],
                "schema": "dbo",
                "tables": dict(JOBS_LAKEHOUSE_TABLES),
            },
            "JobFixtures": {
                "id": job_notebook_item["id"],
                "type": "Notebook",
                "display_name": job_notebook_item["displayName"],
            },
            "TestPipeline": {
                "id": pipeline_item["id"],
                "type": "DataPipeline",
                "display_name": pipeline_item["displayName"],
            },
            "TestSparkJob": {
                "id": spark_job_item["id"],
                "type": "SparkJobDefinition",
                "display_name": spark_job_item["displayName"],
            },
        },
    )
    manifest.write(settings.manifest_path)
    return manifest


def discover_onelake(settings: SandboxSettings) -> SandboxManifest:
    """Write the manifest needed by OneLake tests without unrelated services."""
    workspace_id = settings.require_workspace()
    with FabricApi(get_credential()) as api:
        lakehouse_item = api.find_item(workspace_id, "TestLakehouse", "Lakehouse")
        non_schema_lakehouse_item = api.find_item(
            workspace_id, "TestLakehouseNoSchemas", "Lakehouse"
        )
        warehouse_item = api.find_item(workspace_id, "TestWarehouse", "Warehouse")
        mirrored_database_item = api.find_item(
            workspace_id,
            "TestMirroredDatabase",
            "MirroredDatabase",
        )
        lakehouse = api.get_lakehouse(workspace_id, lakehouse_item["id"])

    revision = verify_fixture_revision(
        settings,
        workspace_id,
        lakehouse_item["id"],
        scope="onelake",
    )
    fixture_contract = read_fixture_contract(
        workspace_id,
        lakehouse_item["id"],
        scope="onelake",
    )
    properties = lakehouse.get("properties", {})
    manifest = SandboxManifest(
        workspace_id=workspace_id,
        workspace_name=settings.workspace_name,
        fixture_revision=revision,
        runtime=fixture_contract["runtime"],
        items={
            "TestLakehouse": {
                "id": lakehouse_item["id"],
                "type": "Lakehouse",
                "display_name": lakehouse_item["displayName"],
                "schema": "dbo",
                "one_lake_files_path": properties.get("oneLakeFilesPath"),
                "one_lake_tables_path": properties.get("oneLakeTablesPath"),
                "tables": dict(ONELAKE_LAKEHOUSE_TABLES),
            },
            "TestWarehouse": {
                "id": warehouse_item["id"],
                "type": "Warehouse",
                "display_name": warehouse_item["displayName"],
                "database_name": warehouse_item["displayName"],
                "tables": {
                    "types": SQL_FIXTURE_TABLE,
                    "mutations": SQL_MUTATION_TABLE,
                },
                "views": {"types": SQL_FIXTURE_VIEW},
            },
            "TestMirroredDatabase": {
                "id": mirrored_database_item["id"],
                "type": "MirroredDatabase",
                "display_name": mirrored_database_item["displayName"],
                "schema": MIRRORED_FIXTURE_SCHEMA,
                "tables": {"types": MIRRORED_FIXTURE_TABLE},
            },
            "TestLakehouseNoSchemas": {
                "id": non_schema_lakehouse_item["id"],
                "type": "Lakehouse",
                "display_name": non_schema_lakehouse_item["displayName"],
                "tables": dict(NON_SCHEMA_LAKEHOUSE_TABLES),
            },
        },
    )
    manifest.write(settings.manifest_path)
    return manifest


def _wait_for_lakehouse_sql_endpoint(
    api: FabricApi,
    workspace_id: str,
    lakehouse_id: str,
    *,
    timeout: int = 600,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        lakehouse = api.get_lakehouse(workspace_id, lakehouse_id)
        endpoint = lakehouse.get("properties", {}).get("sqlEndpointProperties", {})
        if endpoint.get("provisioningStatus") == "Success":
            return lakehouse
        api.sleep(10)
    raise TimeoutError("lakehouse SQL analytics endpoint was not ready in time")


def _wait_for_sql_properties(
    api: FabricApi,
    workspace_id: str,
    item_id: str,
    *,
    item_type: str,
    timeout: int = 900,
) -> dict[str, Any]:
    getter_names = {
        "Warehouse": "get_warehouse",
        "WarehouseSnapshot": "get_warehouse_snapshot",
        "SQLDatabase": "get_sql_database",
        "MirroredDatabase": "get_mirrored_database",
    }
    required_properties = {
        "Warehouse": ("connectionString",),
        "WarehouseSnapshot": (
            "connectionString",
            "parentWarehouseId",
            "snapshotDateTime",
        ),
        "SQLDatabase": ("connectionString", "serverFqdn", "databaseName"),
        "MirroredDatabase": (
            "defaultSchema",
            "oneLakeTablesPath",
            "sqlEndpointProperties",
        ),
    }
    getter = getattr(api, getter_names[item_type])
    required = required_properties[item_type]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        item = getter(workspace_id, item_id)
        properties = item.get("properties", {})
        ready = all(properties.get(name) for name in required)
        if item_type == "MirroredDatabase":
            sql = properties.get("sqlEndpointProperties", {})
            ready = ready and all(
                sql.get(name)
                for name in ("connectionString", "id", "provisioningStatus")
            )
            ready = ready and sql["provisioningStatus"] == "Success"
        if ready:
            return item
        api.sleep(10)
    raise TimeoutError(
        f"{item_type} SQL connection properties were not ready in time"
    )


def _wait_for_kql_properties(
    api: FabricApi,
    workspace_id: str,
    item_id: str,
    *,
    item_type: str,
    timeout: int = 900,
) -> dict[str, Any]:
    getters = {
        "Eventhouse": api.get_eventhouse,
        "KQLDatabase": api.get_kql_database,
    }
    getter = getters[item_type]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        item = getter(workspace_id, item_id)
        properties = item.get("properties", {})
        if properties.get("queryServiceUri"):
            return item
        api.sleep(10)
    raise TimeoutError(f"{item_type} Kusto query service URI was not ready in time")


def _wait_for_environment_publish(
    api: FabricApi,
    workspace_id: str,
    environment_id: str,
    expected_runtime_version: str,
    *,
    timeout: int = 900,
) -> tuple[dict[str, Any], str]:
    deadline = time.monotonic() + timeout
    last_publish_details: dict[str, Any] = {}
    last_runtime_version: Any = None
    failure_states = {"failed", "cancelled"}
    while time.monotonic() < deadline:
        environment = api.get_environment(workspace_id, environment_id)
        publish_details = environment.get("properties", {}).get(
            "publishDetails", {}
        )
        if not isinstance(publish_details, dict):
            publish_details = {}
        component_info = publish_details.get("componentPublishInfo", {})
        if not isinstance(component_info, dict):
            component_info = {}
        spark_settings = component_info.get("sparkSettings", {})
        if not isinstance(spark_settings, dict):
            spark_settings = {}
        publish_state = str(publish_details.get("state", "")).casefold()
        spark_settings_state = str(spark_settings.get("state", "")).casefold()
        if (
            publish_state in failure_states
            or spark_settings_state in failure_states
        ):
            raise RuntimeError(
                "TestEnvironment publish failed: "
                f"{publish_details!r}"
            )
        if publish_state == "success" and spark_settings_state == "success":
            spark_compute = api.get_published_environment_spark_compute(
                workspace_id,
                environment_id,
            )
            last_runtime_version = spark_compute.get("runtimeVersion")
            if last_runtime_version == expected_runtime_version:
                return environment, last_runtime_version
        last_publish_details = publish_details
        remaining = deadline - time.monotonic()
        if remaining > 0:
            api.sleep(min(10, remaining))
    raise TimeoutError(
        "TestEnvironment Spark settings were not published with the expected "
        f"runtime {expected_runtime_version!r} in time; last published runtime: "
        f"{last_runtime_version!r}; last publish details: "
        f"{last_publish_details!r}"
    )


def discover(settings: SandboxSettings) -> SandboxManifest:
    workspace_id = settings.require_workspace()
    with FabricApi(get_credential()) as api:
        lakehouse_item = api.find_item(workspace_id, "TestLakehouse", "Lakehouse")
        non_schema_lakehouse_item = api.find_item(
            workspace_id, "TestLakehouseNoSchemas", "Lakehouse"
        )
        notebook_item = api.find_item(workspace_id, "SeedFixtures", "Notebook")
        job_notebook_item = api.find_item(
            workspace_id, "JobFixtures", "Notebook"
        )
        pipeline_item = api.find_item(
            workspace_id, "TestPipeline", "DataPipeline"
        )
        spark_job_item = api.find_item(
            workspace_id, "TestSparkJob", "SparkJobDefinition"
        )
        environment_item = api.find_item(
            workspace_id, "TestEnvironment", "Environment"
        )
        warehouse_item = api.find_item(
            workspace_id, "TestWarehouse", "Warehouse"
        )
        warehouse_snapshot_item = api.find_item(
            workspace_id,
            "TestWarehouseSnapshot",
            "WarehouseSnapshot",
        )
        sql_database_item = api.find_item(
            workspace_id, "TestSQLDatabase", "SQLDatabase"
        )
        mirrored_database_item = api.find_item(
            workspace_id,
            "TestMirroredDatabase",
            "MirroredDatabase",
        )
        eventhouse_item = api.find_item(
            workspace_id, "TestEventhouse", "Eventhouse"
        )
        kql_database_item = api.find_item(
            workspace_id, "TestKQLDatabase", "KQLDatabase"
        )
        graphql_api_item = api.find_item(
            workspace_id, GRAPHQL_API_NAME, "GraphQLApi"
        )
        lakehouse = _wait_for_lakehouse_sql_endpoint(
            api, workspace_id, lakehouse_item["id"]
        )
        warehouse = _wait_for_sql_properties(
            api,
            workspace_id,
            warehouse_item["id"],
            item_type="Warehouse",
        )
        warehouse_snapshot = _wait_for_sql_properties(
            api,
            workspace_id,
            warehouse_snapshot_item["id"],
            item_type="WarehouseSnapshot",
        )
        sql_database = _wait_for_sql_properties(
            api,
            workspace_id,
            sql_database_item["id"],
            item_type="SQLDatabase",
        )
        mirrored_database = _wait_for_sql_properties(
            api,
            workspace_id,
            mirrored_database_item["id"],
            item_type="MirroredDatabase",
        )
        eventhouse = _wait_for_kql_properties(
            api,
            workspace_id,
            eventhouse_item["id"],
            item_type="Eventhouse",
        )
        kql_database = _wait_for_kql_properties(
            api,
            workspace_id,
            kql_database_item["id"],
            item_type="KQLDatabase",
        )
        environment, environment_runtime_version = (
            _wait_for_environment_publish(
                api,
                workspace_id,
                environment_item["id"],
                settings.spark_runtime_version,
            )
        )
        sql_endpoint_id = lakehouse["properties"]["sqlEndpointProperties"]["id"]
        api.refresh_sql_endpoint_metadata(workspace_id, sql_endpoint_id)

    properties = lakehouse["properties"]
    sql_endpoint = properties["sqlEndpointProperties"]
    warehouse_properties = warehouse["properties"]
    warehouse_snapshot_properties = warehouse_snapshot["properties"]
    sql_database_properties = sql_database["properties"]
    mirrored_database_properties = mirrored_database["properties"]
    eventhouse_properties = eventhouse["properties"]
    kql_database_properties = kql_database["properties"]
    environment_publish_details = environment["properties"]["publishDetails"]
    with PowerBiApi(get_credential()) as power_bi:
        semantic_model = power_bi.find_dataset(
            workspace_id,
            SEMANTIC_MODEL_NAME,
        )
        arrow_semantic_model = power_bi.find_dataset(
            workspace_id,
            ARROW_SEMANTIC_MODEL_NAME,
        )
    revision = verify_fixture_revision(
        settings,
        workspace_id,
        lakehouse_item["id"],
    )
    fixture_contract = read_fixture_contract(
        workspace_id,
        lakehouse_item["id"],
    )
    manifest = SandboxManifest(
        workspace_id=workspace_id,
        workspace_name=settings.workspace_name,
        fixture_revision=revision,
        runtime=fixture_contract["runtime"],
        items={
            "TestLakehouse": {
                "id": lakehouse_item["id"],
                "type": "Lakehouse",
                "display_name": lakehouse_item["displayName"],
                "schema": "dbo",
                "one_lake_files_path": properties.get("oneLakeFilesPath"),
                "one_lake_tables_path": properties.get("oneLakeTablesPath"),
                "sql_endpoint": sql_endpoint.get("connectionString"),
                "sql_endpoint_id": sql_endpoint.get("id"),
                "livy_url": (
                    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
                    f"/lakehouses/{lakehouse_item['id']}"
                    "/livyapi/versions/2023-12-01/sessions"
                ),
                "livy_batch_file": (
                    f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
                    f"{lakehouse_item['id']}/Files/fixtures/livy_batch.py"
                ),
                "tables": {
                    "runtime": "fabricqueryr_runtime",
                    "basic": "fabricqueryr_basic",
                    "non_default_basic": "fabricqueryr_basic",
                    "empty": "fabricqueryr_empty",
                    "void": "fabricqueryr_void",
                    "partitioned": "fabricqueryr_partitioned",
                    "typed_partitions": "fabricqueryr_typed_partitions",
                    "binary_partitions": "fabricqueryr_binary_partitions",
                    "schema_evolved": "fabricqueryr_schema_evolved",
                    "column_mapped": "fabricqueryr_column_mapped",
                    "column_mapped_id": "fabricqueryr_column_mapped_id",
                    "column_mapped_id_partitioned_dv": (
                        "fabricqueryr_column_mapped_id_partitioned_dv"
                    ),
                    "struct_validity": "fabricqueryr_struct_validity",
                    "deletion_vectors": "fabricqueryr_deletion_vectors",
                    "file_row_number_collision": (
                        "fabricqueryr_file_row_number_collision"
                    ),
                    "deletion_vectors_stress": (
                        "fabricqueryr_deletion_vectors_stress"
                    ),
                    "deletion_vectors_checkpoint": (
                        "fabricqueryr_deletion_vectors_checkpoint"
                    ),
                    "deletion_vectors_dense": (
                        "fabricqueryr_deletion_vectors_dense"
                    ),
                    "row_tracking": "fabricqueryr_row_tracking",
                    "exact_types": "fabricqueryr_exact_types",
                    "complex_types": "fabricqueryr_complex_types",
                    "oracle_basic": "fabricqueryr_oracle_basic",
                    "oracle_empty": "fabricqueryr_oracle_empty",
                    "oracle_typed_partitions": (
                        "fabricqueryr_oracle_typed_partitions"
                    ),
                    "oracle_partitioned": "fabricqueryr_oracle_partitioned",
                    "oracle_schema_evolved": (
                        "fabricqueryr_oracle_schema_evolved"
                    ),
                    "oracle_exact_types": "fabricqueryr_oracle_exact_types",
                    "oracle_complex_types": (
                        "fabricqueryr_oracle_complex_types"
                    ),
                    "spark_oracle_column_mapped": (
                        "fabricqueryr_spark_oracle_column_mapped"
                    ),
                    "spark_oracle_column_mapped_id": (
                        "fabricqueryr_spark_oracle_column_mapped_id"
                    ),
                    "spark_oracle_column_mapped_id_partitioned_dv": (
                        "fabricqueryr_spark_oracle_"
                        "column_mapped_id_partitioned_dv"
                    ),
                    "spark_oracle_struct_validity": (
                        "fabricqueryr_spark_oracle_struct_validity"
                    ),
                    "spark_oracle_deletion_vectors": (
                        "fabricqueryr_spark_oracle_deletion_vectors"
                    ),
                    "spark_oracle_file_row_number_collision": (
                        "fabricqueryr_spark_oracle_file_row_number_collision"
                    ),
                    "spark_oracle_deletion_vectors_stress": (
                        "fabricqueryr_spark_oracle_deletion_vectors_stress"
                    ),
                    "spark_oracle_deletion_vectors_checkpoint": (
                        "fabricqueryr_spark_oracle_deletion_vectors_checkpoint"
                    ),
                    "spark_oracle_deletion_vectors_dense": (
                        "fabricqueryr_spark_oracle_deletion_vectors_dense"
                    ),
                    "spark_oracle_row_tracking": (
                        "fabricqueryr_spark_oracle_row_tracking"
                    ),
                    "spark_oracle_type_widened": (
                        "fabricqueryr_spark_oracle_type_widened"
                    ),
                    "spark_oracle_type_widened_exact": (
                        "fabricqueryr_spark_oracle_type_widened_exact"
                    ),
                    "spark_oracle_type_widened_pending": (
                        "fabricqueryr_spark_oracle_type_widened_pending"
                    ),
                    "spark_oracle_type_widened_nested": (
                        "fabricqueryr_spark_oracle_type_widened_nested"
                    ),
                    "spark_oracle_type_widened_map_key": (
                        "fabricqueryr_spark_oracle_type_widened_map_key"
                    ),
                    "spark_oracle_v2_checkpoint": (
                        "fabricqueryr_spark_oracle_v2_checkpoint"
                    ),
                    "spark_oracle_shallow_clone": (
                        "fabricqueryr_spark_oracle_shallow_clone"
                    ),
                    "spark_oracle_variant": (
                        "fabricqueryr_spark_oracle_variant"
                    ),
                    "spark_oracle_variant_id_dv": (
                        "fabricqueryr_spark_oracle_variant_id_dv"
                    ),
                    "shallow_clone": "fabricqueryr_shallow_clone",
                    "type_widened": "fabricqueryr_type_widened",
                    "type_widened_exact": "fabricqueryr_type_widened_exact",
                    "type_widened_pending": (
                        "fabricqueryr_type_widened_pending"
                    ),
                    "type_widened_nested": (
                        "fabricqueryr_type_widened_nested"
                    ),
                    "type_widened_map_key": (
                        "fabricqueryr_type_widened_map_key"
                    ),
                    "v2_checkpoint": "fabricqueryr_v2_checkpoint",
                    "variant": "fabricqueryr_variant",
                    "variant_id_dv": "fabricqueryr_variant_id_dv",
                    "livy_batch_result": "fabricqueryr_livy_batch_result",
                    "spark_job_result": "fabricqueryr_spark_job_result",
                },
            },
            "SeedFixtures": {
                "id": notebook_item["id"],
                "type": "Notebook",
                "display_name": notebook_item["displayName"],
            },
            "TestLakehouseNoSchemas": {
                "id": non_schema_lakehouse_item["id"],
                "type": "Lakehouse",
                "display_name": non_schema_lakehouse_item["displayName"],
                "tables": dict(NON_SCHEMA_LAKEHOUSE_TABLES),
            },
            "JobFixtures": {
                "id": job_notebook_item["id"],
                "type": "Notebook",
                "display_name": job_notebook_item["displayName"],
            },
            "TestPipeline": {
                "id": pipeline_item["id"],
                "type": "DataPipeline",
                "display_name": pipeline_item["displayName"],
            },
            "TestSparkJob": {
                "id": spark_job_item["id"],
                "type": "SparkJobDefinition",
                "display_name": spark_job_item["displayName"],
            },
            "TestEnvironment": {
                "id": environment_item["id"],
                "type": "Environment",
                "display_name": environment_item["displayName"],
                "publish_state": environment_publish_details["state"],
                "spark_settings_state": environment_publish_details[
                    "componentPublishInfo"
                ]["sparkSettings"]["state"],
                "runtime_version": environment_runtime_version,
            },
            "TestWarehouse": {
                "id": warehouse_item["id"],
                "type": "Warehouse",
                "display_name": warehouse_item["displayName"],
                "connection_string": warehouse_properties["connectionString"],
                "database_name": warehouse_item["displayName"],
                "tables": {
                    "types": SQL_FIXTURE_TABLE,
                    "mutations": SQL_MUTATION_TABLE,
                },
                "views": {"types": SQL_FIXTURE_VIEW},
            },
            "TestWarehouseSnapshot": {
                "id": warehouse_snapshot_item["id"],
                "type": "WarehouseSnapshot",
                "display_name": warehouse_snapshot_item["displayName"],
                "connection_string": warehouse_snapshot_properties[
                    "connectionString"
                ],
                "database_name": warehouse_snapshot_item["displayName"],
                "parent_warehouse_id": warehouse_snapshot_properties[
                    "parentWarehouseId"
                ],
                "snapshot_date_time": warehouse_snapshot_properties[
                    "snapshotDateTime"
                ],
            },
            "TestSQLDatabase": {
                "id": sql_database_item["id"],
                "type": "SQLDatabase",
                "display_name": sql_database_item["displayName"],
                "connection_string": sql_database_properties["connectionString"],
                "server_fqdn": sql_database_properties["serverFqdn"],
                "database_name": sql_database_properties["databaseName"],
                "tables": {
                    "types": SQL_FIXTURE_TABLE,
                },
                "views": {"types": SQL_FIXTURE_VIEW},
            },
            "TestMirroredDatabase": {
                "id": mirrored_database_item["id"],
                "type": "MirroredDatabase",
                "display_name": mirrored_database_item["displayName"],
                "connection_string": mirrored_database_properties[
                    "sqlEndpointProperties"
                ]["connectionString"],
                "database_name": mirrored_database_item["displayName"],
                "schema": mirrored_database_properties["defaultSchema"],
                "one_lake_tables_path": mirrored_database_properties[
                    "oneLakeTablesPath"
                ],
                "sql_endpoint_id": mirrored_database_properties[
                    "sqlEndpointProperties"
                ]["id"],
                "tables": {"types": MIRRORED_FIXTURE_TABLE},
            },
            "TestEventhouse": {
                "id": eventhouse_item["id"],
                "type": "Eventhouse",
                "display_name": eventhouse_item["displayName"],
                "query_service_uri": eventhouse_properties["queryServiceUri"],
                "ingestion_service_uri": eventhouse_properties.get(
                    "ingestionServiceUri"
                ),
            },
            "TestKQLDatabase": {
                "id": kql_database_item["id"],
                "type": "KQLDatabase",
                "display_name": kql_database_item["displayName"],
                "database_name": kql_database_item["displayName"],
                "parent_eventhouse_id": kql_database_properties.get(
                    "parentEventhouseItemId"
                ),
                "query_service_uri": kql_database_properties["queryServiceUri"],
                "ingestion_service_uri": kql_database_properties.get(
                    "ingestionServiceUri"
                ),
                "tables": {
                    "events": "fabricqueryr_events",
                    "ingestion": "fabricqueryr_ingestion",
                },
                "mappings": {
                    "ingestion_csv": "fabricqueryr_ingestion_csv",
                },
            },
            "TestSemanticModel": {
                "id": semantic_model["id"],
                "type": "SemanticModel",
                "display_name": SEMANTIC_MODEL_NAME,
                "connection_string": (
                    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
                    f"{quote(settings.workspace_name, safe='')};"
                    f"Initial Catalog={SEMANTIC_MODEL_NAME};"
                ),
            },
            "TestArrowSemanticModel": {
                "id": arrow_semantic_model["id"],
                "type": "SemanticModel",
                "display_name": ARROW_SEMANTIC_MODEL_NAME,
                "connection_string": (
                    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/"
                    f"{quote(settings.workspace_name, safe='')};"
                    f"Initial Catalog={ARROW_SEMANTIC_MODEL_NAME};"
                ),
            },
            "TestGraphQL": {
                "id": graphql_api_item["id"],
                "type": "GraphQLApi",
                "display_name": graphql_api_item["displayName"],
                "endpoint": (
                    f"https://api.fabric.microsoft.com/v1/workspaces/"
                    f"{workspace_id}/graphqlapis/{graphql_api_item['id']}/graphql"
                ),
                "root_field": GRAPHQL_ROOT_FIELD,
                "create_field": GRAPHQL_CREATE_FIELD,
            },
        },
    )
    manifest.write(settings.manifest_path)
    return manifest
