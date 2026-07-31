from pathlib import Path

import pytest

from fabricqueryr_sandbox.fixture_revision import (
    FIXTURE_REVISION_PATH,
    fixture_revision,
    read_fixture_revision,
    verify_fixture_revision,
    write_fixture_revision,
)
from fabricqueryr_sandbox.settings import SandboxSettings


class FakeDownload:
    def __init__(self, payload):
        self.payload = payload

    def readall(self):
        return self.payload


class FakeFile:
    def __init__(self):
        self.payload = b""

    def upload_data(self, payload, *, overwrite):
        assert overwrite is True
        self.payload = payload

    def download_file(self):
        return FakeDownload(self.payload)


class FakeFilesystem:
    def __init__(self):
        self.files = {}

    def get_file_client(self, path):
        return self.files.setdefault(path, FakeFile())


class FakeService:
    def __init__(self):
        self.filesystems = {}

    def get_file_system_client(self, workspace_id):
        return self.filesystems.setdefault(workspace_id, FakeFilesystem())


def make_settings(root: Path) -> SandboxSettings:
    seed_notebook = (
        root
        / "infra"
        / "fabric"
        / "workspace"
        / "SeedFixtures.Notebook"
        / "notebook-content.py"
    )
    seed_notebook.parent.mkdir(parents=True)
    seed_notebook.write_text("seed-v1\n", encoding="utf-8")
    fixture = root / "infra" / "fabric" / "fixtures" / "basic.csv"
    fixture.parent.mkdir(parents=True)
    fixture.write_text("id,name\n1,alpha\n", encoding="utf-8")
    package = (
        root
        / "tools"
        / "fabric-sandbox"
        / "src"
        / "fabricqueryr_sandbox"
    )
    package.mkdir(parents=True)
    for name in ("fixture_revision.py", "seed.py", "sql_api.py"):
        (package / name).write_text(f"{name}-v1\n", encoding="utf-8")
    return SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="lakehouse-id",
        workspace_name="fabricqueryr-test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=root,
        manifest_path=root / "manifest.json",
    )


def test_fixture_revision_changes_with_seed_inputs(tmp_path):
    settings = make_settings(tmp_path)
    first = fixture_revision(settings)

    fixture = settings.fixture_dir / "basic.csv"
    fixture.write_text("id,name\n1,beta\n", encoding="utf-8")
    second = fixture_revision(settings)

    assert len(first) == 64
    assert first != second


def test_fixture_revision_round_trip_and_verification(tmp_path):
    settings = make_settings(tmp_path)
    service = FakeService()
    expected = fixture_revision(settings)

    write_fixture_revision(
        "workspace-id",
        "lakehouse-id",
        expected,
        service_client=service,
    )

    assert (
        read_fixture_revision(
            "workspace-id",
            "lakehouse-id",
            service_client=service,
        )
        == expected
    )
    assert (
        verify_fixture_revision(
            settings,
            "workspace-id",
            "lakehouse-id",
            service_client=service,
        )
        == expected
    )
    marker = (
        service.filesystems["workspace-id"]
        .files[f"lakehouse-id/{FIXTURE_REVISION_PATH}"]
        .payload
    )
    assert marker == f"{expected}\n".encode()

    write_fixture_revision(
        "workspace-id",
        "lakehouse-id",
        "stale",
        service_client=service,
    )
    with pytest.raises(RuntimeError, match="Rebuild or reseed"):
        verify_fixture_revision(
            settings,
            "workspace-id",
            "lakehouse-id",
            service_client=service,
        )
