"""Version and verify the deployed Fabric fixture contract."""

from __future__ import annotations

from hashlib import sha256
from pathlib import Path

from azure.core.credentials import TokenCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient

from .credentials import get_credential
from .settings import SandboxSettings


FIXTURE_REVISION_PATH = "Files/fixtures/fabricqueryr-seed-revision.txt"
INCOMPLETE_FIXTURE_REVISION = "incomplete"


def _fixture_inputs(settings: SandboxSettings) -> list[Path]:
    package_dir = (
        settings.repository_root
        / "tools"
        / "fabric-sandbox"
        / "src"
        / "fabricqueryr_sandbox"
    )
    fixed = [
        settings.workspace_definition_dir
        / "SeedFixtures.Notebook"
        / "notebook-content.py",
        settings.workspace_definition_dir / "parameter.yml",
        package_dir / "deploy.py",
        package_dir / "discover.py",
        package_dir / "fixture_revision.py",
        package_dir / "seed.py",
        package_dir / "sql_api.py",
    ]
    terraform_files = sorted(
        path
        for path in (
            settings.repository_root / "infra" / "fabric" / "terraform"
        ).rglob("*")
        if path.is_file() and path.suffix in {".tf", ".tftpl"}
    )
    fixture_files = sorted(
        path for path in settings.fixture_dir.rglob("*") if path.is_file()
    )
    inputs = fixed + terraform_files + fixture_files
    missing = [path for path in inputs if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Fixture revision input is missing: "
            + ", ".join(str(path) for path in missing)
        )
    return inputs


def fixture_revision(settings: SandboxSettings) -> str:
    """Hash every source that materially defines deployed Delta fixtures."""
    digest = sha256()
    digest.update(b"spark_runtime_version\0")
    digest.update(settings.spark_runtime_version.encode("utf-8"))
    digest.update(b"\0")
    for path in _fixture_inputs(settings):
        relative = path.relative_to(settings.repository_root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _revision_file(
    workspace_id: str,
    lakehouse_id: str,
    *,
    service_client: DataLakeServiceClient | None = None,
    credential: TokenCredential | None = None,
):
    service = service_client or DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=credential or get_credential(),
    )
    filesystem = service.get_file_system_client(workspace_id)
    return filesystem.get_file_client(
        f"{lakehouse_id}/{FIXTURE_REVISION_PATH}"
    )


def write_fixture_revision(
    workspace_id: str,
    lakehouse_id: str,
    revision: str,
    *,
    service_client: DataLakeServiceClient | None = None,
    credential: TokenCredential | None = None,
) -> None:
    """Publish a revision marker after a complete successful seed."""
    if not revision.strip():
        raise ValueError("fixture revision must not be empty")
    _revision_file(
        workspace_id,
        lakehouse_id,
        service_client=service_client,
        credential=credential,
    ).upload_data((revision + "\n").encode("utf-8"), overwrite=True)


def read_fixture_revision(
    workspace_id: str,
    lakehouse_id: str,
    *,
    service_client: DataLakeServiceClient | None = None,
) -> str | None:
    """Read the revision marker currently deployed in OneLake."""
    try:
        payload = _revision_file(
            workspace_id,
            lakehouse_id,
            service_client=service_client,
        ).download_file().readall()
    except ResourceNotFoundError:
        return None
    return payload.decode("utf-8").strip()


def verify_fixture_revision(
    settings: SandboxSettings,
    workspace_id: str,
    lakehouse_id: str,
    *,
    service_client: DataLakeServiceClient | None = None,
) -> str:
    """Fail discovery when the persistent workspace needs rebuilding."""
    expected = fixture_revision(settings)
    actual = read_fixture_revision(
        workspace_id,
        lakehouse_id,
        service_client=service_client,
    )
    if actual != expected:
        deployed = actual or "<missing>"
        raise RuntimeError(
            "Fabric fixture revision mismatch: "
            f"deployed={deployed}, expected={expected}. "
            "Rebuild or reseed the Fabric sandbox before running integration tests."
        )
    return expected
