import json

from azure.core.credentials import AccessToken
import httpx
import pytest

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
        with pytest.raises(RuntimeError, match="job-id.*activity-id.*cancelled by Fabric"):
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
        assert request.url.params["beta"] == "false"
        return httpx.Response(
            200,
            json={
                "id": "job-id",
                "status": "Completed",
                "exitValue": (
                    "fabricqueryr-seed-error: write basic Delta table\n"
                    "AnalysisException: exact Spark failure"
                ),
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
        assert request.url.params["beta"] == "false"
        return httpx.Response(
            200,
            json={
                "id": "job-id",
                "status": "Completed",
                "exitValue": "fabricqueryr-seed-success",
            },
        )

    with FabricApi(StaticCredential(), transport=httpx.MockTransport(handler)) as api:
        result = api.run_notebook(
            "workspace-id",
            "notebook-id",
            lakehouse_id="lakehouse-id",
        )

    assert result["exitValue"] == "fabricqueryr-seed-success"


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
    responses = iter(
        [
            httpx.Response(
                202,
                headers={"Location": "/operations/operation-id"},
            ),
            httpx.Response(200, json={"status": "Running"}),
            httpx.Response(200, json={"status": "Succeeded"}),
        ]
    )

    def handler(request):
        response = next(responses)
        if request.method == "POST":
            assert request.url.path.endswith(
                "/workspaces/workspace-id/sqlEndpoints/endpoint-id/refreshMetadata"
            )
        else:
            assert request.url.path.endswith("/operations/operation-id")
        return response

    with FabricApi(
        StaticCredential(),
        transport=httpx.MockTransport(handler),
        sleep=lambda _: None,
    ) as api:
        result = api.refresh_sql_endpoint_metadata(
            "workspace-id", "endpoint-id"
        )

    assert result["status"] == "Succeeded"
