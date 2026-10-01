import shutil
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pyarrow as pa
import pytest
from azure.core.exceptions import ResourceNotFoundError
from deltalake import DeltaTable, write_deltalake
from fabricqueryr_sandbox import persistent_ci, persistent_sandbox, reset_ci, storage


class LocalOneLake:
    """Execute the storage algorithm on real files, including real Delta history."""

    def __init__(self, root):
        self.root = root.resolve()
        self.listed = []

    def path(self, name):
        resolved = (self.root / name).resolve()
        assert resolved.is_relative_to(self.root) and resolved != self.root
        return resolved

    def get_file_system_client(self, workspace_id):
        assert workspace_id == "workspace"
        return self

    def get_paths(self, *, path, recursive):
        assert recursive is False
        self.listed.append(path)
        directory = self.path(path)
        if not directory.exists():
            raise ResourceNotFoundError("absent")
        for child in sorted(directory.iterdir()):
            yield SimpleNamespace(
                name=child.relative_to(self.root).as_posix(),
                is_directory=child.is_dir(),
                content_length=child.stat().st_size if child.is_file() else 0,
            )

    def get_directory_client(self, path):
        target = self.path(path)
        return SimpleNamespace(delete_directory=lambda: shutil.rmtree(target))

    def get_file_client(self, path):
        target = self.path(path)
        return SimpleNamespace(delete_file=lambda: target.unlink())


def fabric(links=()):
    api = MagicMock()
    api.list_items.return_value = [
        {"id": "lakehouse", "type": "Lakehouse", "displayName": "TestLakehouse"},
        {
            "id": "no-schema",
            "type": "Lakehouse",
            "displayName": "TestLakehouseNoSchemas",
        },
        {"id": "other", "type": "Lakehouse", "displayName": "UserLakehouse"},
    ]
    api.request.return_value = httpx.Response(200, json={"value": list(links)})
    return api


def file(root, path, data=b"fixture"):
    destination = root / path
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(data)
    return destination


def test_repeated_real_delta_writes_are_reclaimed_and_fixture_history_survives(
    tmp_path,
):
    service, api = LocalOneLake(tmp_path), fabric()
    fixture = tmp_path / "lakehouse/Tables/dbo/fabricqueryr_basic"
    first = pa.table({"id": [1, 2, 3]})
    write_deltalake(fixture, first)
    write_deltalake(fixture, pa.table({"id": [4]}), mode="overwrite")
    marker = file(tmp_path, "lakehouse/Files/fixtures/fabricqueryr-ci-targets.json")
    user = file(tmp_path, "lakehouse/Files/user-data/keep.csv")
    before = storage.check_storage(api, service, "workspace")
    for _ in range(3):
        for lakehouse, schema in [("lakehouse", "dbo/"), ("no-schema", "")]:
            path = tmp_path / f"{lakehouse}/Tables/{schema}fabricqueryr_r_load"
            write_deltalake(path, first)
            write_deltalake(path, pa.table({"id": [9]}), mode="overwrite")
            assert len(list(path.glob("*.parquet"))) >= 2
            file(
                tmp_path,
                f"{lakehouse}/Files/fabricqueryr-staging/failed-load/part.parquet",
            )
            reset_ci.clean_files(api, service, "workspace", lakehouse)
            assert not path.exists()
            assert not (tmp_path / f"{lakehouse}/Files/fabricqueryr-staging").exists()
        after = storage.check_storage(api, service, "workspace")
        assert after["bytes"] == before["bytes"]
        assert DeltaTable(fixture, version=0).to_pyarrow_table().to_pydict() == {
            "id": [1, 2, 3]
        }
        assert marker.read_bytes() == user.read_bytes() == b"fixture"
    # Retrying cleanup after an interrupted workflow is harmless.
    reset_ci.clean_files(api, service, "workspace", "lakehouse")
    assert storage.check_storage(api, service, "workspace")["bytes"] == before["bytes"]


def test_cleanup_only_removes_known_demo_outputs(tmp_path):
    service, api = LocalOneLake(tmp_path), fabric()
    remove = [
        "Files/playground/fabricqueryr-demo-20261002090000-123.csv",
        "Files/playground/kql-export-abcd/part.parquet",
        "Tables/dbo/fabricqueryr_playground_orders/_delta_log/0000.json",
    ]
    keep = [
        "Files/playground/my-analysis.csv",
        "Tables/dbo/my_orders/data.parquet",
        "Tables/custom/fabricqueryr_r_load/data.parquet",
        "Tables/dbo/fabricqueryr_partitioned/_delta_log/0000.json",
    ]
    for path in remove + keep:
        file(tmp_path, "lakehouse/" + path)
    reset_ci.clean_files(api, service, "workspace", "lakehouse")
    assert all(not (tmp_path / "lakehouse" / p).exists() for p in remove)
    assert all((tmp_path / "lakehouse" / p).exists() for p in keep)


def test_shortcuts_are_not_traversed_and_deletion_waits_for_next_pass(tmp_path):
    service = LocalOneLake(tmp_path)
    paths = [
        "Tables/dbo/fabricqueryr_r_load",
        "Files/fabricqueryr-staging/external",
        "Files/personal-link",
    ]
    links = [{"path": p.rsplit("/", 1)[0], "name": p.rsplit("/", 1)[1]} for p in paths]
    api = fabric(links)
    for path in paths:
        file(tmp_path, f"lakehouse/{path}/external-data.bin", b"x" * 10000)
    reset_ci.clean_files(api, service, "workspace", "lakehouse")
    assert all(
        (tmp_path / "lakehouse" / p / "external-data.bin").exists() for p in paths
    )
    removed = [c.args[1] for c in api.request.call_args_list if c.args[0] == "DELETE"]
    assert len(removed) == 2 and not any("personal-link" in p for p in removed)
    result = storage.check_storage(api, service, "workspace")
    assert result["bytes"] == 0
    assert all(
        not any(storage.inside(p, "lakehouse/" + link) for link in paths)
        for p in service.listed
    )


def test_schema_shortcut_is_never_scanned_or_deleted(tmp_path):
    service = LocalOneLake(tmp_path)
    api = fabric([{"path": "Tables", "name": "dbo"}])
    target = file(tmp_path, "lakehouse/Tables/dbo/fabricqueryr_r_load/part.parquet")
    reset_ci.clean_files(api, service, "workspace", "lakehouse")
    assert storage.check_storage(api, service, "workspace")["bytes"] == 0
    assert target.exists()
    assert "lakehouse/Tables/dbo" not in service.listed
    assert not any(c.args[0] == "DELETE" for c in api.request.call_args_list)


def test_budget_counts_retained_and_unknown_files_but_never_deletes_them(
    tmp_path, monkeypatch
):
    service, api = LocalOneLake(tmp_path), fabric()
    old = file(
        tmp_path, "lakehouse/Tables/dbo/fabricqueryr_basic/obsolete.parquet", b"x" * 30
    )
    user = file(tmp_path, "no-schema/Files/personal.csv", b"x" * 40)
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("FABRIC_SANDBOX_MAX_STORAGE_BYTES", "60")
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    with pytest.raises(RuntimeError, match="budget exceeded"):
        storage.check_storage(api, service, "workspace")
    assert old.exists() and user.exists()
    assert (
        "70 bytes" in summary.read_text()
        and "scan stopped early" in summary.read_text()
    )


def test_entry_budget_stops_listing_before_unbounded_scan(tmp_path, monkeypatch):
    service, api = LocalOneLake(tmp_path), fabric()
    for i in range(20):
        file(tmp_path, f"lakehouse/Files/{i}.txt", b"")
    monkeypatch.setenv("FABRIC_SANDBOX_MAX_STORAGE_ENTRIES", "5")
    with pytest.raises(RuntimeError, match="budget exceeded"):
        storage.check_storage(api, service, "workspace")
    assert "no-schema/Files" not in service.listed


@pytest.mark.parametrize(
    "path",
    [
        "lakehouse/Files/../other",
        "/other",
        "lakehouse\\Files",
        "lakehouse/Files/%2e%2e/other",
        "other/Files/fabricqueryr-staging",
    ],
)
def test_malformed_or_out_of_scope_listing_cannot_delete_data(path):
    service, api = MagicMock(), fabric()
    service.get_file_system_client.return_value.get_paths.return_value = [
        SimpleNamespace(name=path, is_directory=True)
    ]
    with pytest.raises(ValueError):
        reset_ci.clean_files(api, service, "workspace", "lakehouse")
    service.get_file_system_client.return_value.get_directory_client.assert_not_called()


def test_storage_permission_errors_are_not_reported_as_empty():
    service, api = MagicMock(), fabric()
    service.get_file_system_client.return_value.get_paths.side_effect = PermissionError(
        "denied"
    )
    with pytest.raises(PermissionError):
        storage.check_storage(api, service, "workspace")


def test_interactive_maintenance_cleans_before_budget_check(tmp_path, monkeypatch):
    service, api = LocalOneLake(tmp_path), fabric()
    file(tmp_path, "lakehouse/Tables/dbo/fabricqueryr_r_load/part.parquet", b"x" * 100)
    monkeypatch.setenv("FABRIC_SANDBOX_MAX_STORAGE_BYTES", "50")
    monkeypatch.setattr(
        persistent_sandbox, "DataLakeServiceClient", lambda **_: nullcontext(service)
    )
    # No active Fabric compute in this local execution. The file cleanup and
    # size calculation both execute against disk.
    monkeypatch.setattr(reset_ci, "quiesce_jobs", lambda *_: None)
    monkeypatch.setattr(reset_ci, "quiesce_livy", lambda *_: None)
    persistent_sandbox.maintain(api, object(), "workspace")
    assert not (tmp_path / "lakehouse/Tables/dbo/fabricqueryr_r_load").exists()


def test_all_writers_stop_before_any_storage_is_removed(monkeypatch):
    api = fabric()
    events = []
    monkeypatch.setattr(reset_ci, "quiesce_jobs", lambda *_: events.append("jobs"))
    monkeypatch.setattr(
        reset_ci, "quiesce_livy", lambda _a, _w, lake: events.append("stop:" + lake)
    )
    monkeypatch.setattr(
        reset_ci, "clean_files", lambda _a, _s, _w, lake: events.append("clean:" + lake)
    )
    reset_ci.clean_workspace(api, object(), object(), "workspace")
    assert events == [
        "jobs",
        "stop:lakehouse",
        "stop:no-schema",
        "clean:lakehouse",
        "clean:no-schema",
    ]


@pytest.mark.parametrize("action", ["prepare", "clean"])
def test_ci_entry_point_cleans_before_audit_and_only_then_prepares(
    tmp_path, monkeypatch, action
):
    service, api = LocalOneLake(tmp_path), fabric()
    file(tmp_path, "lakehouse/Tables/dbo/fabricqueryr_r_load/part.parquet", b"x" * 100)
    monkeypatch.setenv("FABRIC_SANDBOX_MAX_STORAGE_BYTES", "50")
    monkeypatch.setattr(persistent_ci, "get_credential", lambda: object())
    monkeypatch.setattr(persistent_ci, "FabricApi", lambda _: nullcontext(api))
    monkeypatch.setattr(
        persistent_ci, "DataLakeServiceClient", lambda **_: nullcontext(service)
    )
    settings = SimpleNamespace(capacity_id="capacity", spark_runtime_lane="core")
    monkeypatch.setattr(
        persistent_ci.SandboxSettings, "from_environment", lambda: settings
    )
    monkeypatch.setattr(persistent_ci, "wait_active_capacity", lambda *_: None)
    verified = []

    def owned(*_):
        verified.append(True)
        return {"id": "workspace"}

    def stopped(*_):
        assert verified

    prepared = MagicMock(return_value=SimpleNamespace(workspace_id="workspace"))
    monkeypatch.setattr(persistent_ci, "owned_workspace", owned)
    monkeypatch.setattr(persistent_ci, "prepare", prepared)
    monkeypatch.setattr(reset_ci, "quiesce_jobs", stopped)
    monkeypatch.setattr(reset_ci, "quiesce_livy", stopped)
    persistent_ci.main([action, "--repository", "owner/repo"])
    assert not (tmp_path / "lakehouse/Tables/dbo/fabricqueryr_r_load").exists()
    assert prepared.call_count == int(action == "prepare")


def test_interactive_storage_failure_stops_before_reseeding(monkeypatch):
    settings = SimpleNamespace(capacity_id="capacity")
    monkeypatch.setattr(
        persistent_sandbox.shiny_sandbox, "wait_active_capacity", lambda *_: None
    )
    monkeypatch.setattr(
        persistent_sandbox,
        "ensure_workspace",
        lambda *_args, **_kwargs: {"id": "workspace"},
    )
    monkeypatch.setattr(
        persistent_sandbox,
        "maintain",
        MagicMock(side_effect=RuntimeError("budget exceeded")),
    )
    prepare = MagicMock()
    monkeypatch.setattr(persistent_sandbox.shiny_sandbox, "prepare_workspace", prepare)
    with pytest.raises(RuntimeError, match="budget exceeded"):
        persistent_sandbox.prepare(
            object(), object(), settings, "owner/repo", "owner", "shiny"
        )
    prepare.assert_not_called()
