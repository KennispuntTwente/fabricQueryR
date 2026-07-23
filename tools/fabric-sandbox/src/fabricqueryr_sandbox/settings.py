"""Environment-backed sandbox configuration."""

from __future__ import annotations

from dataclasses import dataclass
from os import environ
from pathlib import Path


@dataclass(frozen=True)
class SandboxSettings:
    """Configuration shared by local and CI sandbox commands."""

    workspace_id: str | None
    lakehouse_id: str | None
    workspace_name: str
    capacity_id: str | None
    principal_id: str | None
    environment: str
    repository_root: Path
    manifest_path: Path

    @classmethod
    def from_environment(cls) -> "SandboxSettings":
        repository_root = Path(__file__).resolve().parents[4]
        return cls(
            workspace_id=environ.get("FABRIC_WORKSPACE_ID"),
            lakehouse_id=environ.get("FABRIC_LAKEHOUSE_ID"),
            workspace_name=environ.get(
                "FABRIC_WORKSPACE_NAME", "fabricqueryr-local"
            ),
            capacity_id=environ.get("FABRIC_CAPACITY_ID"),
            principal_id=environ.get("FABRIC_CI_PRINCIPAL_ID"),
            environment=environ.get("FABRIC_DEPLOYMENT_ENVIRONMENT", "TEST"),
            repository_root=repository_root,
            manifest_path=Path(
                environ.get(
                    "FABRIC_TEST_MANIFEST",
                    repository_root / ".fabric-test-manifest.json",
                )
            ),
        )

    @property
    def workspace_definition_dir(self) -> Path:
        return self.repository_root / "infra" / "fabric" / "workspace"

    @property
    def fixture_dir(self) -> Path:
        return self.repository_root / "infra" / "fabric" / "fixtures"

    @property
    def item_types(self) -> list[str]:
        return ["Notebook"]

    def require_workspace(self) -> str:
        if not self.workspace_id:
            raise ValueError(
                "FABRIC_WORKSPACE_ID is required; use the Terraform workspace_id output"
            )
        return self.workspace_id

    def require_lakehouse(self) -> str:
        if not self.lakehouse_id:
            raise ValueError(
                "FABRIC_LAKEHOUSE_ID is required; use the Terraform lakehouse_id output"
            )
        return self.lakehouse_id

    def validate_local_paths(self) -> list[str]:
        missing = []
        for path in (self.workspace_definition_dir, self.fixture_dir):
            if not path.is_dir():
                missing.append(str(path))
        return missing