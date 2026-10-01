"""Reap interrupted tests inside an already verified integration workspace."""

import re
from urllib.parse import quote

import httpx
import pyodbc
from azure.core.exceptions import ResourceNotFoundError

from .fabric_api import TERMINAL_JOB_STATES
from .kusto_api import KustoApi, INGESTION_TABLE_COMMAND
from .shiny_sandbox import complete_operation
from .sql_api import SQL_AUDIENCE, _odbc_connection_settings

JOB_TYPES = {
    "Notebook": "RunNotebook",
    "DataPipeline": "Execute",
    "SparkJobDefinition": "SparkJob",
}
LIVY_TERMINAL = {"dead", "error", "killed", "success"}
TEMP_NOTEBOOK = re.compile(r"^fabricqueryr_(probe|operation|jupyter)_[A-Za-z0-9_-]+$")
TEMP_SQL = re.compile(
    r"^(fabricqueryr_(r_write|adbc_write|case_[0-9]+|unsigned_[0-9]+|timestamp_[0-9]+|narrowing_[0-9]+|dictionary_[0-9]+|records_[0-9]+)|rollback_[0-9a-f]+)$"
)
TEMP_KQL = re.compile(
    r"^fabricqueryr_(mixed|cleanup|decimal|r_create|storage|unsigned)_[A-Za-z0-9_-]+$"
)
TEMP_FILES = re.compile(
    r"^fabricqueryr(?:-(?:tests|staging|decimal-export|kql-export)$|-(?:datetime|livy-languages|livy-dependencies|encoding|collision)-[A-Za-z0-9_-]+$|_(?:dependencies|job_override|discovery|case|mixed|cleanup|csv_transform)_[A-Za-z0-9_-]+$|_recovery$)"
)
TEMP_SHORTCUT = re.compile(
    r"^fabricqueryr_(shortcut_live|table_shortcut_[A-Za-z0-9_-]+|bulk_[A-Za-z0-9_-]+|csv_transform_[A-Za-z0-9_-]+|external_[A-Za-z0-9_-]+|case_[A-Za-z0-9_-]+)$"
)


def pages(api, path):
    values = []
    seen = set()
    while path:
        if path in seen:
            raise RuntimeError("Fabric returned a repeated continuation URI")
        seen.add(path)
        page = api.request("GET", path).json()
        values.extend(page["value"])
        path = page.get("continuationUri")
    return values


def quiesce_jobs(api, workspace_id, items):
    pending = []
    for item in items:
        job_type = JOB_TYPES.get(item["type"])
        if not job_type:
            continue
        root = f"/workspaces/{workspace_id}/items/{item['id']}"
        schedules = f"{root}/jobs/{job_type}/schedules"
        for schedule in pages(api, schedules):
            api.request("DELETE", f"{schedules}/{schedule['id']}")
        for job in pages(api, f"{root}/jobs/instances"):
            if job["status"] not in TERMINAL_JOB_STATES:
                path = f"{root}/jobs/instances/{job['id']}"
                try:
                    api.request("POST", f"{path}/cancel")
                except httpx.HTTPStatusError as error:
                    # Completion may win the cancellation race; verify below.
                    if error.response.status_code not in {400, 409}:
                        raise
                pending.append(path)
    for _ in range(12):
        pending = [
            path
            for path in pending
            if api.request("GET", path).json()["status"] not in TERMINAL_JOB_STATES
        ]
        if not pending:
            return
        api.sleep(5)
    raise TimeoutError("Interrupted item jobs did not stop; refusing fixture reset")


def quiesce_livy(api, workspace_id, lakehouse_id):
    root = f"/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/livyapi/versions/2023-12-01"
    for kind in ("sessions", "batches"):
        offset, sessions = 0, []
        while True:
            payload = api.request(
                "GET", f"{root}/{kind}", params={"from": offset, "size": 100}
            ).json()
            batch = payload["sessions"]
            sessions.extend(batch)
            offset += len(batch)
            if offset >= payload["total"]:
                break
            if not batch:
                raise RuntimeError("Livy pagination did not advance")
        pending = []
        for session in sessions:
            if session["state"].lower() not in LIVY_TERMINAL:
                path = f"{root}/{kind}/{session['id']}"
                api.request("DELETE", path)
                pending.append(path)
        for _ in range(12):
            active = []
            for path in pending:
                try:
                    state = api.request("GET", path).json()["state"].lower()
                except httpx.HTTPStatusError as error:
                    if error.response.status_code != 404:
                        raise
                    continue
                if state not in LIVY_TERMINAL:
                    active.append(path)
            pending = active
            if not pending:
                break
            api.sleep(5)
        if pending:
            raise TimeoutError(
                "Interrupted Livy compute did not stop; refusing fixture reset"
            )


def clean_files(api, service, workspace_id, lakehouse_id):
    root = f"/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts"
    # Remove links via the shortcut API, never recursively through their target.
    for shortcut in pages(api, root):
        path, name = shortcut["path"], shortcut["name"]
        components = path.split("/")
        inside_scratch = (
            len(components) > 1
            and components[0] == "Files"
            and TEMP_FILES.fullmatch(components[1])
            and ".." not in components
        )
        if inside_scratch or (
            path in {"Files", "Tables", "Tables/dbo"} and TEMP_SHORTCUT.fullmatch(name)
        ):
            api.request(
                "DELETE", f"{root}/{quote(path, safe='/')}/{quote(name, safe='')}"
            )
    filesystem = service.get_file_system_client(workspace_id)
    try:
        paths = list(
            filesystem.get_paths(path=f"{lakehouse_id}/Files", recursive=False)
        )
    except ResourceNotFoundError:
        return
    for entry in paths:
        name = entry.name.rsplit("/", 1)[-1]
        if TEMP_FILES.fullmatch(name):
            if entry.is_directory:
                filesystem.get_directory_client(entry.name).delete_directory()
            else:
                filesystem.get_file_client(entry.name).delete_file()


def clean_sql(api, credential, workspace_id, item):
    kind = item["type"]
    route = "warehouses" if kind == "Warehouse" else "sqlDatabases"
    properties = api.request(
        "GET", f"/workspaces/{workspace_id}/{route}/{item['id']}"
    ).json()["properties"]
    if not properties.get("connectionString"):
        return  # An interrupted first creation has no SQL fixture to reset yet.
    database = properties.get("databaseName", item["displayName"])
    connection_string, attributes = _odbc_connection_settings(
        properties["connectionString"],
        database,
        credential.get_token(SQL_AUDIENCE).token,
    )
    connection = pyodbc.connect(
        connection_string, attrs_before=attributes, autocommit=True
    )
    try:
        connection.timeout = 30
        cursor = connection.cursor()
        tables = [
            row[0]
            for row in cursor.execute(
                "SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('dbo')"
            ).fetchall()
        ]
        for table in tables:
            if TEMP_SQL.fullmatch(table):
                cursor.execute(f"DROP TABLE [dbo].[{table}]")
        if "fabricqueryr_graphql" in tables:
            cursor.execute("DELETE FROM dbo.fabricqueryr_graphql WHERE id = -99")
    finally:
        connection.close()


def clean_kql(api, credential, workspace_id, item):
    properties = api.get_kql_database(workspace_id, item["id"])["properties"]
    endpoint = properties.get("queryServiceUri")
    if not endpoint:
        return
    with KustoApi(credential) as kusto:
        result = kusto.execute_management(
            endpoint,
            item["displayName"],
            ".show tables | project TableName",
            max_attempts=3,
        )
        names = [
            row[0]
            for table in result.get("Tables", [])
            if table.get("TableName") != "QueryStatus"
            for row in table.get("Rows", [])
        ]
        for name in names:
            if isinstance(name, str) and TEMP_KQL.fullmatch(name):
                kusto.execute_management(
                    endpoint,
                    item["displayName"],
                    f".drop table {name} ifexists",
                    max_attempts=3,
                )
        if "fabricqueryr_ingestion" in names:
            kusto.execute_management(
                endpoint, item["displayName"], INGESTION_TABLE_COMMAND, max_attempts=3
            )


def clean_workspace(api, credential, service, workspace_id, *, scratch=True):
    """Caller must validate workspace ownership before calling this function."""
    items = api.list_items(workspace_id)
    quiesce_jobs(api, workspace_id, items)
    for item in items:
        if item["type"] == "Lakehouse" and item["displayName"] in {
            "TestLakehouse",
            "TestLakehouseNoSchemas",
        }:
            quiesce_livy(api, workspace_id, item["id"])
            if scratch:
                clean_files(api, service, workspace_id, item["id"])
        if not scratch:
            continue
        if item["type"] == "Notebook" and TEMP_NOTEBOOK.fullmatch(item["displayName"]):
            complete_operation(
                api,
                api.request("DELETE", f"/workspaces/{workspace_id}/items/{item['id']}"),
                "Delete interrupted probe",
            )
        elif item["type"] in {"Warehouse", "SQLDatabase"} and item["displayName"] in {
            "TestWarehouse",
            "TestSQLDatabase",
        }:
            clean_sql(api, credential, workspace_id, item)
        elif item["type"] == "KQLDatabase" and item["displayName"] == "TestKQLDatabase":
            clean_kql(api, credential, workspace_id, item)
