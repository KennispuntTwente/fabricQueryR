import base64
import json

from azure.core.credentials import AccessToken
import httpx
import pytest

import fabricqueryr_sandbox.fabric_api as fabric_api
from fabricqueryr_sandbox.fabric_api import FabricApi


class StaticCredential:
    def get_token(self, *_scopes, **_kwargs):
        return AccessToken("test-token", 4_102_444_800)


def test_list_items_follows_continuation_uri():
    def handler(request):
        assert request.headers["Authorization"] == "Bearer test-token"
        if "page=2" in str(request.url):
            return httpx.Response(200, json={"value": [{"id": "two"}]})
        return httpx.Response(
            200,
            json={
                "value": [{"id": "one"}],
                "continuationUri": "https://api.fabric.microsoft.com/v1/items?page=2",
            },
        )

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        items = api.list_items("workspace-id")

    assert [item["id"] for item in items] == ["one", "two"]


def test_continuation_uri_cannot_forward_token_to_another_origin():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "value": [{"id": "one"}],
                "continuationUri": "https://attacker.example/items?page=2",
            },
        )

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        with pytest.raises(ValueError, match="configured HTTPS origin"):
            api.list_items("workspace-id")

    assert len(requests) == 1
    assert requests[0].headers["Authorization"] == "Bearer test-token"


def test_request_retries_transient_responses_and_honors_retry_after():
    attempts = 0
    sleeps = []

    def handler(_request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        if attempts == 2:
            return httpx.Response(503)
        return httpx.Response(200, json={"value": []})

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        response = api.request("GET", "/workspaces")

    assert response.status_code == 200
    assert attempts == 3
    assert sleeps == [3, 1]


def test_request_does_not_retry_ambiguous_post_server_errors():
    attempts = 0

    def handler(_request):
        nonlocal attempts
        attempts += 1
        return httpx.Response(503)

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
    ) as api:
        with pytest.raises(httpx.HTTPStatusError, match="503"):
            api.request("POST", "/workspaces/workspace-id/items")

    assert attempts == 1


def test_request_retries_connection_establishment_failures():
    attempts = 0
    sleeps = []

    def handler(request):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"value": []})

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        response = api.request("GET", "/workspaces")

    assert response.status_code == 200
    assert attempts == 2
    assert sleeps == [0.5]


def test_request_does_not_retry_ambiguous_read_timeouts():
    attempts = 0

    def handler(request):
        nonlocal attempts
        attempts += 1
        raise httpx.ReadTimeout("response timed out", request=request)

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
    ) as api:
        with pytest.raises(httpx.ReadTimeout, match="response timed out"):
            api.request("POST", "/workspaces/workspace-id/items")

    assert attempts == 1


def test_list_workspaces_filters_admin_role_and_follows_continuation():
    requests = []

    def handler(request):
        requests.append(request)
        if "page=2" in str(request.url):
            return httpx.Response(200, json={"value": [{"id": "two"}]})
        return httpx.Response(
            200,
            json={
                "value": [{"id": "one"}],
                "continuationUri": (
                    "https://api.fabric.microsoft.com/v1/workspaces?page=2"
                ),
            },
        )

    with FabricApi(
        StaticCredential(), transport=httpx.MockTransport(handler)
    ) as api:
        workspaces = api.list_workspaces()

    assert [workspace["id"] for workspace in workspaces] == ["one", "two"]
    assert requests[0].url.params["roles"] == "Admin"
    assert "roles" not in requests[1].url.params


def test_delete_workspace_uses_core_workspace_route():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200)

    with FabricApi(
        StaticCredential(), transport=httpx.MockTransport(handler)
    ) as api:
        api.delete_workspace("workspace-id")

    assert requests[0].method == "DELETE"
    assert requests[0].url.path == "/v1/workspaces/workspace-id"


def test_configure_workspace_spark_runtime_updates_an_unbound_workspace():
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(
                200,
                json={
                    "environment": {
                        "name": "",
                        "runtimeVersion": "1.3",
                    }
                },
            )
        return httpx.Response(
            200,
            json={
                "environment": {
                    "name": "",
                    "runtimeVersion": "2.0",
                }
            },
        )

    with FabricApi(
        StaticCredential(), transport=httpx.MockTransport(handler)
    ) as api:
        result = api.configure_workspace_spark_runtime("workspace-id", "2.0")

    assert result["environment"]["runtimeVersion"] == "2.0"
    assert [request.method for request in requests] == ["GET", "PATCH"]
    assert json.loads(requests[1].content) == {
        "environment": {"name": "", "runtimeVersion": "2.0"}
    }


def test_configure_workspace_spark_runtime_is_idempotent():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(
            200,
            json={
                "environment": {
                    "name": "",
                    "runtimeVersion": "2.0",
                }
            },
        )

    with FabricApi(
        StaticCredential(), transport=httpx.MockTransport(handler)
    ) as api:
        result = api.configure_workspace_spark_runtime("workspace-id", "2.0")

    assert result["environment"]["runtimeVersion"] == "2.0"
    assert [request.method for request in requests] == ["GET"]


def test_configure_workspace_spark_runtime_preserves_named_environments():
    def handler(_request):
        return httpx.Response(
            200,
            json={
                "environment": {
                    "name": "managed-runtime",
                    "runtimeVersion": "1.3",
                }
            },
        )

    with FabricApi(
        StaticCredential(), transport=httpx.MockTransport(handler)
    ) as api:
        with pytest.raises(RuntimeError, match="managed-runtime"):
            api.configure_workspace_spark_runtime("workspace-id", "2.0")


def test_find_item_rejects_ambiguous_names():
    def handler(_request):
        return httpx.Response(
            200,
            json={
                "value": [
                    {"displayName": "Same", "type": "Lakehouse"},
                    {"displayName": "Same", "type": "Lakehouse"},
                ]
            },
        )

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        try:
            api.find_item("workspace-id", "Same", "Lakehouse")
        except RuntimeError as error:
            assert "found 2" in str(error)
        else:
            raise AssertionError("ambiguous item lookup should fail")


def test_get_workload_items_uses_typed_routes():
    paths = []

    def handler(request):
        paths.append(request.url.path)
        return httpx.Response(200, json={"id": request.url.path.rsplit("/", 1)[-1]})

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        environment = api.get_environment("workspace-id", "environment-id")
        warehouse = api.get_warehouse("workspace-id", "warehouse-id")
        warehouse_snapshot = api.get_warehouse_snapshot(
            "workspace-id", "snapshot-id"
        )
        sql_database = api.get_sql_database("workspace-id", "database-id")
        mirrored_database = api.get_mirrored_database(
            "workspace-id", "mirrored-database-id"
        )
        eventhouse = api.get_eventhouse("workspace-id", "eventhouse-id")
        kql_database = api.get_kql_database("workspace-id", "kql-database-id")
        graphql_api = api.get_graphql_api("workspace-id", "graphql-api-id")

    assert environment["id"] == "environment-id"
    assert warehouse["id"] == "warehouse-id"
    assert warehouse_snapshot["id"] == "snapshot-id"
    assert sql_database["id"] == "database-id"
    assert mirrored_database["id"] == "mirrored-database-id"
    assert eventhouse["id"] == "eventhouse-id"
    assert kql_database["id"] == "kql-database-id"
    assert graphql_api["id"] == "graphql-api-id"
    assert paths == [
        "/v1/workspaces/workspace-id/environments/environment-id",
        "/v1/workspaces/workspace-id/warehouses/warehouse-id",
        "/v1/workspaces/workspace-id/warehouseSnapshots/snapshot-id",
        "/v1/workspaces/workspace-id/sqlDatabases/database-id",
        (
            "/v1/workspaces/workspace-id/mirroredDatabases/"
            "mirrored-database-id"
        ),
        "/v1/workspaces/workspace-id/eventhouses/eventhouse-id",
        "/v1/workspaces/workspace-id/kqlDatabases/kql-database-id",
        "/v1/workspaces/workspace-id/graphQLApis/graphql-api-id",
    ]


def test_get_published_environment_spark_compute_uses_stable_contract():
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"runtimeVersion": "2.0"})

    with FabricApi(
        StaticCredential(), transport=httpx.MockTransport(handler)
    ) as api:
        result = api.get_published_environment_spark_compute(
            "workspace-id",
            "environment-id",
        )

    assert result["runtimeVersion"] == "2.0"
    assert requests[0].url.path == (
        "/v1/workspaces/workspace-id/environments/environment-id/sparkcompute"
    )
    assert requests[0].url.params["beta"] == "false"


def test_wait_for_mirroring_running_starts_and_polls_until_ready():
    statuses = iter(["Initialized", "Starting", "Running"])
    requests = []
    sleeps = []

    def handler(request):
        requests.append(request.url.path)
        if request.url.path.endswith("/getMirroringStatus"):
            return httpx.Response(200, json={"status": next(statuses)})
        assert request.url.path.endswith("/startMirroring")
        return httpx.Response(200)

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        result = api.wait_for_mirroring_running(
            "workspace-id",
            "mirrored-database-id",
        )

    assert result == {"status": "Running"}
    assert sum(path.endswith("/startMirroring") for path in requests) == 1
    assert sum(path.endswith("/getMirroringStatus") for path in requests) == 3
    assert sleeps == [10, 10]


def test_update_graphql_definition_encodes_supported_public_definition():
    requests = []
    definition = {
        "$schema": "https://example.test/schema.json",
        "datasources": [{"sourceItemId": "sql-endpoint-id"}],
    }

    def handler(request):
        requests.append(request)
        return httpx.Response(200)

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        result = api.update_graphql_definition(
            "workspace-id",
            "graphql-api-id",
            definition,
        )

    assert result == {"status": "Succeeded"}
    request = requests[0]
    assert request.url.path.endswith(
        "/workspaces/workspace-id/graphQLApis/graphql-api-id/updateDefinition"
    )
    payload = json.loads(request.content)
    assert "format" not in payload["definition"]
    part = payload["definition"]["parts"][0]
    assert part["path"] == "graphql-definition.json"
    assert part["payloadType"] == "InlineBase64"
    decoded = json.loads(base64.b64decode(part["payload"]))
    assert decoded == definition


def test_update_graphql_definition_polls_validated_operation_id():
    operation_id = "31b2a510-1703-4fc6-b5f2-a8a314ebcce3"
    requests = []

    def handler(request):
        requests.append(request)
        if request.method == "POST":
            return httpx.Response(
                202,
                headers={
                    "Location": "https://regional.fabric.example/operations/ignored",
                    "x-ms-operation-id": operation_id,
                },
            )
        assert request.url == httpx.URL(
            f"https://api.fabric.microsoft.com/v1/operations/{operation_id}"
        )
        return httpx.Response(200, json={"status": "Succeeded"})

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
    ) as api:
        result = api.update_graphql_definition(
            "workspace-id",
            "graphql-api-id",
            {"datasources": []},
        )

    assert result == {"status": "Succeeded"}
    assert len(requests) == 2


def test_update_graphql_definition_rejects_invalid_operation_id():
    def handler(_request):
        return httpx.Response(
            202,
            headers={
                "Location": "/operations/operation-id",
                "x-ms-operation-id": "../unsafe",
            },
        )

    with FabricApi(
        StaticCredential(), transport=httpx.MockTransport(handler)
    ) as api:
        with pytest.raises(RuntimeError, match="invalid x-ms-operation-id"):
            api.update_graphql_definition(
                "workspace-id",
                "graphql-api-id",
                {"datasources": []},
            )


def test_request_preserves_fabric_error_details():
    def handler(request):
        return httpx.Response(
            400,
            headers={"x-ms-request-id": "request-id"},
            json={
                "errorCode": "InvalidDefinitionFormat",
                "message": "Requested item definition format is invalid",
            },
        )

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        with pytest.raises(httpx.HTTPStatusError) as caught:
            api.request("POST", "/workspaces/workspace-id/items")

    message = str(caught.value)
    assert "InvalidDefinitionFormat" in message
    assert "Requested item definition format is invalid" in message
    assert "Request ID: request-id" in message


def test_wait_for_graphql_root_field_retries_until_schema_is_ready():
    attempts = 0
    sleeps = []

    def handler(request):
        nonlocal attempts
        attempts += 1
        payload = json.loads(request.content)
        assert "__type" not in payload["query"]
        assert "fabricqueryr_basics(first: 1)" in payload["query"]
        assert "variables" not in payload
        if attempts == 1:
            return httpx.Response(503, json={"message": "schema is provisioning"})
        if attempts == 2:
            return httpx.Response(
                200,
                json={
                    "data": {"fabricqueryr_basics": None},
                    "errors": [{"message": "Schema is provisioning"}],
                },
            )
        return httpx.Response(
            200,
            json={
                "data": {
                    "fabricqueryr_basics": {"items": [{"id": 1}]}
                }
            },
        )

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        result = api.wait_for_graphql_root_field(
            "workspace-id",
            "graphql-api-id",
            "fabricqueryr_basics",
        )

    assert result["name"] == "fabricqueryr_basics"
    assert attempts == 3
    assert sleeps == [10, 10]


def test_wait_for_graphql_root_field_rejects_unsafe_names():
    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(lambda _request: httpx.Response(200)),
    ) as api:
        with pytest.raises(ValueError, match="root_field.*valid GraphQL name"):
            api.wait_for_graphql_root_field(
                "workspace-id",
                "graphql-api-id",
                "root { __schema }",
            )


def test_run_notebook_reports_cancelled_job_trace_ids():
    def handler(request):
        if request.method == "POST":
            return httpx.Response(
                202,
                headers={"Location": "/jobs/instances/job-id"},
            )
        return httpx.Response(
            200,
            json={
                "id": "job-id",
                "status": "Cancelled",
                "rootActivityId": "activity-id",
                "failureReason": {"message": "cancelled by Fabric"},
            },
        )

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        with pytest.raises(
            RuntimeError,
            match="job-id.*activity-id.*cancelled by Fabric",
        ):
            api.run_notebook(
                "workspace-id",
                "notebook-id",
                lakehouse_id="lakehouse-id",
            )


def test_run_notebook_surfaces_seed_traceback_from_exit_value():
    def handler(request):
        if request.method == "POST":
            return httpx.Response(
                202,
                headers={"Location": "/jobs/instances/job-id"},
            )
        assert request.url.params["beta"] == "true"
        assert request.url.path.endswith(
            "/workspaces/workspace-id/notebooks/notebook-id/"
            "jobs/execute/instances/job-id"
        )
        return httpx.Response(
            200,
            json={
                "id": "job-id",
                "status": "Completed",
                "properties": {
                    "exitValue": (
                        "fabricqueryr-seed-error: write basic Delta table\n"
                        "AnalysisException: exact Spark failure"
                    )
                },
            },
        )

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        with pytest.raises(RuntimeError, match="exact Spark failure"):
            api.run_notebook(
                "workspace-id",
                "notebook-id",
                lakehouse_id="lakehouse-id",
            )


def test_run_notebook_binds_lakehouse_and_requires_success_marker():
    def handler(request):
        if request.method == "POST":
            assert request.url.path.endswith(
                "/workspaces/workspace-id/notebooks/notebook-id/"
                "jobs/execute/instances"
            )
            assert request.url.params["beta"] == "false"
            payload = json.loads(request.content)
            assert payload["executionData"]["compute"] == "Spark"
            assert payload["executionData"]["computeConfiguration"][
                "defaultLakehouse"
            ] == {
                "referenceType": "ById",
                "itemId": "lakehouse-id",
                "workspaceId": "workspace-id",
            }
            return httpx.Response(
                202,
                headers={"Location": "/jobs/instances/job-id"},
            )
        assert request.url.params["beta"] == "true"
        assert request.url.path.endswith(
            "/workspaces/workspace-id/notebooks/notebook-id/"
            "jobs/execute/instances/job-id"
        )
        return httpx.Response(
            200,
            json={
                "id": "job-id",
                "status": "Completed",
                "properties": {
                    "exitValue": "fabricqueryr-seed-success:{}",
                },
            },
        )

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        result = api.run_notebook(
            "workspace-id",
            "notebook-id",
            lakehouse_id="lakehouse-id",
        )

    assert result["exitValue"] == "fabricqueryr-seed-success:{}"


def test_run_notebook_retries_until_job_instance_is_visible():
    get_attempts = 0
    sleeps = []

    def handler(request):
        nonlocal get_attempts
        if request.method == "POST":
            return httpx.Response(
                202,
                headers={"Location": "/jobs/instances/job-id"},
            )
        get_attempts += 1
        if get_attempts == 1:
            return httpx.Response(
                404,
                headers={"Retry-After": "3"},
                json={"errorCode": "ItemNotFound"},
            )
        return httpx.Response(
            200,
            json={
                "id": "job-id",
                "status": "Completed",
                "exitValue": "fabricqueryr-seed-success:{}",
            },
        )

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        result = api.run_notebook(
            "workspace-id",
            "notebook-id",
            lakehouse_id="lakehouse-id",
        )

    assert result["exitValue"] == "fabricqueryr-seed-success:{}"
    assert get_attempts == 2
    assert sleeps == [3]


def test_run_notebook_honors_submission_and_status_retry_after():
    get_attempts = 0
    sleeps = []

    def handler(request):
        nonlocal get_attempts
        if request.method == "POST":
            return httpx.Response(
                202,
                headers={
                    "Location": "/jobs/instances/job-id",
                    "Retry-After": "7",
                },
            )
        get_attempts += 1
        if get_attempts == 1:
            return httpx.Response(
                200,
                headers={"Retry-After": "13"},
                json={"id": "job-id", "status": "InProgress"},
            )
        return httpx.Response(
            200,
            json={
                "id": "job-id",
                "status": "Completed",
                "exitValue": "fabricqueryr-seed-success:{}",
            },
        )

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        result = api.run_notebook(
            "workspace-id",
            "notebook-id",
            lakehouse_id="lakehouse-id",
        )

    assert result["exitValue"] == "fabricqueryr-seed-success:{}"
    assert sleeps == [7, 13]


def test_run_notebook_stops_retrying_persistent_not_found(monkeypatch):
    get_attempts = 0
    sleeps = []
    monkeypatch.setattr(fabric_api, "JOB_VISIBILITY_RETRIES", 2)

    def handler(request):
        nonlocal get_attempts
        if request.method == "POST":
            return httpx.Response(
                202,
                headers={"Location": "/jobs/instances/job-id"},
            )
        get_attempts += 1
        return httpx.Response(404, json={"errorCode": "ItemNotFound"})

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        with pytest.raises(httpx.HTTPStatusError, match="404 Not Found"):
            api.run_notebook(
                "workspace-id",
                "notebook-id",
                lakehouse_id="lakehouse-id",
            )

    assert get_attempts == 3
    assert sleeps == [fabric_api.JOB_VISIBILITY_RETRY_SECONDS] * 2


def test_run_notebook_does_not_retry_other_http_errors():
    get_attempts = 0
    sleeps = []

    def handler(request):
        nonlocal get_attempts
        if request.method == "POST":
            return httpx.Response(
                202,
                headers={"Location": "/jobs/instances/job-id"},
            )
        get_attempts += 1
        return httpx.Response(403, json={"errorCode": "Forbidden"})

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        with pytest.raises(httpx.HTTPStatusError, match="403 Forbidden"):
            api.run_notebook(
                "workspace-id",
                "notebook-id",
                lakehouse_id="lakehouse-id",
            )

    assert get_attempts == 1
    assert sleeps == []


def test_run_notebook_rejects_missing_success_marker():
    def handler(request):
        if request.method == "POST":
            return httpx.Response(
                202,
                headers={"Location": "/jobs/instances/job-id"},
            )
        return httpx.Response(
            200,
            json={"id": "job-id", "status": "Completed"},
        )

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        with pytest.raises(RuntimeError, match="without its success marker"):
            api.run_notebook(
                "workspace-id",
                "notebook-id",
                lakehouse_id="lakehouse-id",
            )


def test_refresh_sql_endpoint_metadata_waits_for_success():
    statuses = {
        "value": [
            {
                "tableName": "dbo.fabricqueryr_basic",
                "status": "Success",
            }
        ]
    }
    responses = iter(
        [
            httpx.Response(
                202,
                headers={"Location": "/operations/operation-id"},
            ),
            httpx.Response(200, json={"status": "Running"}),
            httpx.Response(200, json={"status": "Succeeded"}),
            httpx.Response(200, json=statuses),
        ]
    )

    def handler(request):
        response = next(responses)
        if request.method == "POST":
            assert request.url.path.endswith(
                "/workspaces/workspace-id/sqlEndpoints/endpoint-id/refreshMetadata"
            )
            payload = json.loads(request.content)
            assert payload == {
                "recreateTables": True,
                "timeout": {"value": 120, "timeUnit": "Seconds"},
            }
        elif request.url.path.endswith("/result"):
            assert request.url.path == "/v1/operations/operation-id/result"
        else:
            assert request.url.path.endswith("/operations/operation-id")
        return response

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
    ) as api:
        result = api.refresh_sql_endpoint_metadata(
            "workspace-id",
            "endpoint-id",
            timeout=120,
            recreate_tables=True,
        )

    assert result == statuses


def test_wait_for_sql_endpoint_table_retries_not_run_then_succeeds():
    requests = []
    sleeps = []
    responses = iter(
        [
            {
                "value": [
                    {
                        "tableName": "dbo.fabricqueryr_basic",
                        "status": "NotRun",
                    }
                ]
            },
            {
                "value": [
                    {
                        "tableName": "[dbo].[fabricqueryr_basic]",
                        "status": "Success",
                    }
                ]
            },
        ]
    )

    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json=next(responses))

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
    ) as api:
        status = api.wait_for_sql_endpoint_table(
            "workspace-id",
            "endpoint-id",
            "dbo.fabricqueryr_basic",
        )

    assert status["status"] == "Success"
    assert requests[0]["recreateTables"] is False
    assert requests[1]["recreateTables"] is True
    assert sleeps == [15]


def test_wait_for_sql_endpoint_table_surfaces_table_sync_failure():
    requests = []

    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(
            200,
            json={
                "value": [
                    {
                        "tableName": "dbo.fabricqueryr_basic",
                        "status": "Failure",
                        "error": {
                            "errorCode": "TableSyncFailed",
                            "message": "Delta metadata is invalid",
                        },
                    }
                ]
            },
        )

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
    ) as api:
        with pytest.raises(RuntimeError, match="clean rebuild.*TableSyncFailed"):
            api.wait_for_sql_endpoint_table(
                "workspace-id",
                "endpoint-id",
                "dbo.fabricqueryr_basic",
            )

    assert [request["recreateTables"] for request in requests] == [False, True]


def test_wait_for_sql_endpoint_table_requires_documented_status_result():
    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={"status": "Succeeded"},
            )
        ),
    ) as api:
        with pytest.raises(RuntimeError, match="sync statuses"):
            api.wait_for_sql_endpoint_table(
                "workspace-id",
                "endpoint-id",
                "dbo.fabricqueryr_basic",
            )
