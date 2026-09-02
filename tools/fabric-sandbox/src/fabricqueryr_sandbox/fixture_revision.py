"""Version and verify the deployed Fabric fixture contract."""

from __future__ import annotations

import json
from collections.abc import Iterable
from hashlib import sha256
from pathlib import Path

from azure.core.credentials import TokenCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.filedatalake import DataLakeServiceClient

from .credentials import get_credential
from .settings import SandboxSettings


FIXTURE_REVISION_PATH = "Files/fixtures/fabricqueryr-seed-revision.txt"
ONELAKE_FIXTURE_REVISION_PATH = (
    "Files/fixtures/fabricqueryr-onelake-seed-revision.txt"
)
JOBS_FIXTURE_REVISION_PATH = (
    "Files/fixtures/fabricqueryr-jobs-seed-revision.txt"
)
INCOMPLETE_FIXTURE_REVISION = "incomplete"
FIXTURE_SCOPES = ("all", "onelake", "jobs")
TEXT_FIXTURE_INPUT_NAMES = frozenset({".platform"})
TEXT_FIXTURE_INPUT_SUFFIXES = frozenset(
    {
        ".bim",
        ".csv",
        ".json",
        ".pbism",
        ".py",
        ".tf",
        ".tftpl",
        ".txt",
        ".yml",
        ".yaml",
    }
)


def _validate_scope(scope: str) -> None:
    if scope not in FIXTURE_SCOPES:
        raise ValueError(
            f"Unknown fixture scope {scope!r}; expected one of "
            + ", ".join(FIXTURE_SCOPES)
        )


def _fixture_input_bytes(path: Path) -> bytes:
    content = path.read_bytes()
    if (
        path.name in TEXT_FIXTURE_INPUT_NAMES
        or path.suffix.lower() in TEXT_FIXTURE_INPUT_SUFFIXES
    ):
        return content.replace(b"\r\n", b"\n")
    return content


def _sorted_fixture_paths(
    paths: Iterable[Path],
    repository_root: Path,
) -> list[Path]:
    return sorted(
        paths,
        key=lambda path: path.relative_to(repository_root).as_posix(),
    )


def _fixture_inputs(
    settings: SandboxSettings,
    *,
    scope: str = "all",
) -> list[Path]:
    _validate_scope(scope)
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
        package_dir / "open_mirroring.py",
        package_dir / "power_bi_api.py",
        package_dir / "seed.py",
        package_dir / "sql_api.py",
    ]
    if scope == "jobs":
        fixed = [
            package_dir / "fixture_revision.py",
            package_dir / "seed.py",
        ]
        workspace_inputs = [
            settings.workspace_definition_dir / "parameter.yml",
            *(
                settings.workspace_definition_dir / item
                for item in (
                    "SeedFixtures.Notebook",
                    "JobFixtures.Notebook",
                    "TestPipeline.DataPipeline",
                    "TestSparkJob.SparkJobDefinition",
                    "TestEnvironment.Environment",
                )
            ),
        ]
        workspace_files = sorted(
            path
            for source in workspace_inputs
            for path in (
                source.rglob("*") if source.is_dir() else (source,)
            )
            if path.is_file()
            and "__pycache__" not in path.parts
            and path.suffix != ".pyc"
        )
        terraform_files = []
    else:
        if scope == "onelake":
            fixed = [
                package_dir / "deploy.py",
                package_dir / "discover.py",
                package_dir / "fixture_revision.py",
                package_dir / "open_mirroring.py",
                package_dir / "seed.py",
                package_dir / "sql_api.py",
            ]
            workspace_inputs = [
                settings.workspace_definition_dir / "parameter.yml",
                settings.workspace_definition_dir / "SeedFixtures.Notebook",
            ]
            workspace_files = sorted(
                path
                for source in workspace_inputs
                for path in (
                    source.rglob("*") if source.is_dir() else (source,)
                )
                if path.is_file()
                and "__pycache__" not in path.parts
                and path.suffix != ".pyc"
            )
        else:
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
            if path.is_file() and path.suffix in {".json", ".tf", ".tftpl"}
        )
    fixture_files = sorted(
        path for path in settings.fixture_dir.rglob("*") if path.is_file()
    )
    workspace_files = _sorted_fixture_paths(
        workspace_files,
        settings.repository_root,
    )
    terraform_files = _sorted_fixture_paths(
        terraform_files,
        settings.repository_root,
    )
    fixture_files = _sorted_fixture_paths(
        fixture_files,
        settings.repository_root,
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
    *,
    scope: str = "all",
) -> str:
    """Hash every source that materially defines deployed Delta fixtures."""
    _validate_scope(scope)
    digest = sha256()
    digest.update(b"fixture_scope\0")
    digest.update(scope.encode("utf-8"))
    digest.update(b"\0")
    digest.update(b"spark_runtime_lane\0")
    digest.update(settings.spark_runtime_lane.encode("utf-8"))
    digest.update(b"\0")
    digest.update(b"spark_runtime_version\0")
    digest.update(settings.spark_runtime_version.encode("utf-8"))
    digest.update(b"\0")
    if scope == "all":
        digest.update(b"provision_sql_database\0")
        digest.update(str(settings.provision_sql_database).encode("ascii"))
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
    for path in _fixture_inputs(settings, scope=scope):
        relative = path.relative_to(settings.repository_root).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(_fixture_input_bytes(path))
        digest.update(b"\0")
    return digest.hexdigest()


def _revision_file(
    workspace_id: str,
    lakehouse_id: str,
    *,
    service_client: DataLakeServiceClient | None = None,
    credential: TokenCredential | None = None,
    scope: str = "all",
):
    _validate_scope(scope)
    service = service_client or DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=credential or get_credential(),
    )
    filesystem = service.get_file_system_client(workspace_id)
    marker_path = {
        "all": FIXTURE_REVISION_PATH,
        "onelake": ONELAKE_FIXTURE_REVISION_PATH,
        "jobs": JOBS_FIXTURE_REVISION_PATH,
    }[scope]
    return filesystem.get_file_client(f"{lakehouse_id}/{marker_path}")


def write_fixture_revision(
    workspace_id: str,
    lakehouse_id: str,
    revision: str,
    *,
    runtime_contract: dict[str, str] | None = None,
    service_client: DataLakeServiceClient | None = None,
    credential: TokenCredential | None = None,
    scope: str = "all",
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
        scope=scope,
    ).upload_data(
        (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8"),
        overwrite=True,
    )


def read_fixture_contract(
    workspace_id: str,
    lakehouse_id: str,
    *,
    service_client: DataLakeServiceClient | None = None,
    scope: str = "all",
) -> dict[str, object] | None:
    """Read the deployed source revision and observed runtime build."""
    try:
        payload = _revision_file(
            workspace_id,
            lakehouse_id,
            service_client=service_client,
            scope=scope,
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
    scope: str = "all",
) -> str | None:
    """Read the revision marker currently deployed in OneLake."""
    contract = read_fixture_contract(
        workspace_id,
        lakehouse_id,
        service_client=service_client,
        scope=scope,
    )
    revision = contract.get("revision") if contract else None
    return revision if isinstance(revision, str) else None


def verify_fixture_revision(
    settings: SandboxSettings,
    workspace_id: str,
    lakehouse_id: str,
    *,
    service_client: DataLakeServiceClient | None = None,
    scope: str = "all",
) -> str:
    """Fail discovery when the persistent workspace needs rebuilding."""
    contract = read_fixture_contract(
        workspace_id,
        lakehouse_id,
        service_client=service_client,
        scope=scope,
    )
    runtime = contract.get("runtime") if contract else None
    actual = contract.get("revision") if contract else None
    if not isinstance(runtime, dict):
        runtime = None
    expected = fixture_revision(settings, runtime, scope=scope)
    runtime_matches = runtime is not None and (
        runtime.get("lane") == settings.spark_runtime_lane
        and runtime.get("fabric_runtime") == settings.spark_runtime_version
    )
    if actual != expected or not runtime_matches:
        deployed = actual or "<missing>"
        raise RuntimeError(
            f"Fabric {scope} fixture revision mismatch: "
            f"deployed={deployed}, expected={expected}. "
            "Rebuild or reseed the Fabric sandbox before running integration tests."
        )
    return expected
