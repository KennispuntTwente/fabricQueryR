from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from fabricqueryr_sandbox import reset_ci as reset
from fabricqueryr_sandbox.fabric_api import FabricApi
from test_shiny_sandbox import Credential


def test_schedules_and_active_jobs_are_stopped_before_reset():
    requests = []

    def handle(request):
        method, path = request.method, request.url.path
        requests.append((method, path))
        if method == "GET" and path.endswith("/schedules"):
            return httpx.Response(200, json={"value": [{"id": "schedule"}]})
        if method == "GET" and path.endswith("/instances"):
            return httpx.Response(
                200,
                json={
                    "value": [{"id": "done", "status": "Completed"}],
                    "continuationUri": "/next",
                },
            )
        if path == "/v1/next":
            return httpx.Response(
                200, json={"value": [{"id": "running", "status": "InProgress"}]}
            )
        if method == "GET" and path.endswith("/running"):
            return httpx.Response(200, json={"status": "Cancelled"})
        return httpx.Response(204)

    with FabricApi(Credential(), transport=httpx.MockTransport(handle)) as api:
        reset.quiesce_jobs(api, "workspace", [{"id": "notebook", "type": "Notebook"}])
    mutations = [(m, p.rsplit("/", 1)[-1]) for m, p in requests if m != "GET"]
    assert mutations == [("DELETE", "schedule"), ("POST", "cancel")]
    assert requests[-1][1].endswith("/running")


def test_unstopped_job_prevents_scratch_reset():
    api = MagicMock()
    api.request.side_effect = [
        httpx.Response(200, json={"value": []}),
        httpx.Response(200, json={"value": [{"id": "job", "status": "InProgress"}]}),
        httpx.Response(202),
        *[httpx.Response(200, json={"status": "InProgress"}) for _ in range(12)],
    ]
    with pytest.raises(TimeoutError, match="refusing fixture reset"):
        reset.quiesce_jobs(api, "workspace", [{"id": "notebook", "type": "Notebook"}])


def test_livy_collects_pages_before_deleting_and_waits_for_terminal_state():
    removed = []

    def handle(request):
        path = request.url.path
        if request.method == "DELETE":
            removed.append(path)
            return httpx.Response(200, json={"msg": "deleted"})
        if path.endswith("/sessions"):
            page = int(request.url.params["from"])
            return httpx.Response(
                200,
                json={
                    "total": 2,
                    "sessions": [
                        {"id": page + 1, "state": "idle" if page == 0 else "dead"}
                    ],
                },
            )
        if path.endswith("/batches"):
            return httpx.Response(
                200, json={"total": 1, "sessions": [{"id": 3, "state": "running"}]}
            )
        return httpx.Response(200, json={"state": "killed"})

    with FabricApi(Credential(), transport=httpx.MockTransport(handle)) as api:
        reset.quiesce_livy(api, "workspace", "lakehouse")
    assert [p.rsplit("/", 2)[-2:] for p in removed] == [
        ["sessions", "1"],
        ["batches", "3"],
    ]


def test_files_cleanup_preserves_fixtures_markers_and_unknown_data():
    api, service = MagicMock(), MagicMock()
    filesystem = service.get_file_system_client.return_value
    names = [
        "fixtures",
        "fabricqueryr-deployed-JobFixtures.Notebook.json",
        "user-data",
        "fabricqueryr-tests",
        "fabricqueryr-staging",
        "fabricqueryr_case_123",
    ]
    entries = [
        SimpleNamespace(name=f"lakehouse/Files/{name}", is_directory=True)
        for name in names
    ]
    filesystem.get_paths.side_effect = lambda *, path, recursive: (
        entries if path == "lakehouse/Files" else []
    )
    api.request.return_value = httpx.Response(
        200,
        json={
            "value": [
                {"path": "Files/fabricqueryr_case_123", "name": "ExternalTarget"},
                {"path": "Files", "name": "user-link"},
            ]
        },
    )
    reset.clean_files(api, service, "workspace", "lakehouse")
    deleted = [call.args[0] for call in filesystem.get_directory_client.call_args_list]
    # The parent of a just-deleted shortcut waits until the next reset, because
    # the link deletion may not yet be visible through OneLake.
    assert deleted == [f"lakehouse/Files/{name}" for name in names[3:5]]
    api.request.assert_any_call(
        "DELETE",
        "/workspaces/workspace/items/lakehouse/shortcuts/Files/fabricqueryr_case_123/ExternalTarget",
    )
    assert not any(
        "user-link" in call.args[1] and call.args[0] == "DELETE"
        for call in api.request.call_args_list
    )


def test_sql_cleanup_drops_only_test_scratch_and_graphql_sentinel(monkeypatch):
    api = MagicMock()
    api.request.return_value = httpx.Response(
        200,
        json={"properties": {"connectionString": "example.sql.fabric.microsoft.com"}},
    )
    connection = MagicMock()
    cursor = connection.cursor.return_value
    cursor.execute.return_value.fetchall.return_value = [
        (name,)
        for name in [
            "fabricqueryr_sql_types",
            "fabricqueryr_sql_mutations",
            "fabricqueryr_graphql",
            "fabricqueryr_records_123",
            "user_table",
            "rollback_abcdef",
        ]
    ]
    monkeypatch.setattr(reset.pyodbc, "connect", lambda *_args, **_kwargs: connection)
    reset.clean_sql(
        api,
        Credential(),
        "workspace",
        {"id": "warehouse", "type": "Warehouse", "displayName": "TestWarehouse"},
    )
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[1:] == [
        "DROP TABLE [dbo].[fabricqueryr_records_123]",
        "DROP TABLE [dbo].[rollback_abcdef]",
        "DELETE FROM dbo.fabricqueryr_graphql WHERE id = -99",
    ]
    connection.close.assert_called_once()


@pytest.mark.parametrize(
    "name",
    [
        "fabricqueryr_basic",
        "fabricqueryr_sql_types",
        "fabricqueryr_sql_mutations",
        "fabricqueryr_events",
        "fabricqueryr_timestamps",
    ],
)
def test_baseline_tables_are_never_scratch(name):
    assert not reset.TEMP_SQL.fullmatch(name)
    assert not reset.TEMP_KQL.fullmatch(name)


def test_post_test_cleanup_only_stops_compute(monkeypatch):
    api = MagicMock()
    api.list_items.return_value = [
        {"id": "lakehouse", "displayName": "TestLakehouse", "type": "Lakehouse"}
    ]
    jobs, livy, files = MagicMock(), MagicMock(), MagicMock()
    monkeypatch.setattr(reset, "quiesce_jobs", jobs)
    monkeypatch.setattr(reset, "quiesce_livy", livy)
    monkeypatch.setattr(reset, "clean_files", files)
    reset.clean_workspace(api, Credential(), MagicMock(), "workspace", scratch=False)
    jobs.assert_called_once()
    livy.assert_called_once()
    files.assert_not_called()
