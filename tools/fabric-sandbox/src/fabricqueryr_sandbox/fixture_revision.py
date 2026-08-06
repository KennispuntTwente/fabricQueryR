"""Version and verify the deployed Fabric fixture contract."""

from __future__ import annotations

import json
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
        package_dir / "deploy.py",
        package_dir / "discover.py",
        package_dir / "fixture_revision.py",
        package_dir / "graphql_api.py",
        package_dir / "kusto_api.py",
        package_dir / "power_bi_api.py",
        package_dir / "seed.py",
        package_dir / "sql_api.py",
    ]
    workspace_files = sorted(
        path
        for path in settings.workspace_definition_dir.rglob("*")
        if path.is_file()
        and "__pycache__" not in path.parts
        and path.suffix != ".pyc"
    )
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
    inputs = fixed + workspace_files + terraform_files + fixture_files
    missing = [path for path in inputs if not path.is_file()]
    if missing:
        raise FileNotFoundError(
            "Fixture revision input is missing: "
            + ", ".join(str(path) for path in missing)
        )
    return inputs


def fixture_revision(
    settings: SandboxSettings,
    runtime_contract: dict[str, str] | None = None,
) -> str:
    """Hash every source that materially defines deployed Delta fixtures."""
    digest = sha256()
    digest.update(b"spark_runtime_lane\0")
    digest.update(settings.spark_runtime_lane.encode("utf-8"))
    digest.update(b"\0")
    digest.update(b"spark_runtime_version\0")
    digest.update(settings.spark_runtime_version.encode("utf-8"))
    digest.update(b"\0")
    if runtime_contract is not None:
        digest.update(b"observed_runtime_contract\0")
        digest.update(
            json.dumps(
                runtime_contract,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        )
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
    runtime_contract: dict[str, str] | None = None,
    service_client: DataLakeServiceClient | None = None,
    credential: TokenCredential | None = None,
) -> None:
    """Publish a revision marker after a complete successful seed."""
    if not revision.strip():
        raise ValueError("fixture revision must not be empty")
    payload = {
        "revision": revision,
        "runtime": runtime_contract,
    }
    _revision_file(
        workspace_id,
        lakehouse_id,
        service_client=service_client,
        credential=credential,
    ).upload_data(
        (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"),
        overwrite=True,
    )


def read_fixture_contract(
    workspace_id: str,
    lakehouse_id: str,
    *,
    service_client: DataLakeServiceClient | None = None,
) -> dict[str, object] | None:
    """Read the deployed source revision and observed runtime build."""
    try:
        payload = _revision_file(
            workspace_id,
            lakehouse_id,
            service_client=service_client,
        ).download_file().readall()
    except ResourceNotFoundError:
        return None
    try:
        contract = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(contract, dict):
        return None
    return contract


def read_fixture_revision(
    workspace_id: str,
    lakehouse_id: str,
    *,
    service_client: DataLakeServiceClient | None = None,
) -> str | None:
    """Read the revision marker currently deployed in OneLake."""
    contract = read_fixture_contract(
        workspace_id,
        lakehouse_id,
        service_client=service_client,
    )
    revision = contract.get("revision") if contract else None
    return revision if isinstance(revision, str) else None


def verify_fixture_revision(
    settings: SandboxSettings,
    workspace_id: str,
    lakehouse_id: str,
    *,
    service_client: DataLakeServiceClient | None = None,
) -> str:
    """Fail discovery when the persistent workspace needs rebuilding."""
    contract = read_fixture_contract(
        workspace_id,
        lakehouse_id,
        service_client=service_client,
    )
    runtime = contract.get("runtime") if contract else None
    actual = contract.get("revision") if contract else None
    if not isinstance(runtime, dict):
        runtime = None
    expected = fixture_revision(settings, runtime)
    runtime_matches = runtime is not None and (
        runtime.get("lane") == settings.spark_runtime_lane
        and runtime.get("fabric_runtime") == settings.spark_runtime_version
    )
    if actual != expected or not runtime_matches:
        deployed = actual or "<missing>"
        raise RuntimeError(
            "Fabric fixture revision mismatch: "
            f"deployed={deployed}, expected={expected}. "
            "Rebuild or reseed the Fabric sandbox before running integration tests."
        )
    return expected
