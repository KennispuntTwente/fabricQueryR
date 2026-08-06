"""Environment-backed sandbox configuration."""

from __future__ import annotations

from dataclasses import dataclass
from os import environ
from pathlib import Path

RUNTIME_LANES = {"core": "1.3", "preview": "2.0"}


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
    spark_runtime_lane: str = "core"
    spark_runtime_version: str = "1.3"
    non_schema_lakehouse_id: str | None = None

    def __post_init__(self) -> None:
        expected = RUNTIME_LANES.get(self.spark_runtime_lane)
        if expected is None:
            raise ValueError("FABRIC_SPARK_RUNTIME_LANE must be core or preview")
        if self.spark_runtime_version != expected:
            raise ValueError(
                f"runtime lane {self.spark_runtime_lane!r} requires Fabric "
                f"Runtime {expected}, got {self.spark_runtime_version}"
            )

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
            spark_runtime_lane=environ.get(
                "FABRIC_SPARK_RUNTIME_LANE", "core"
            ),
            spark_runtime_version=environ.get(
                "FABRIC_SPARK_RUNTIME_VERSION", "1.3"
            ),
            non_schema_lakehouse_id=environ.get(
                "FABRIC_NON_SCHEMA_LAKEHOUSE_ID"
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
        return [
            "Notebook",
            "DataPipeline",
            "SparkJobDefinition",
            "SemanticModel",
        ]

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

    def require_non_schema_lakehouse(self) -> str:
        if not self.non_schema_lakehouse_id:
            raise ValueError(
                "FABRIC_NON_SCHEMA_LAKEHOUSE_ID is required; use the Terraform "
                "non_schema_lakehouse_id output"
            )
        return self.non_schema_lakehouse_id

    def validate_local_paths(self) -> list[str]:
        missing = []
        for path in (self.workspace_definition_dir, self.fixture_dir):
            if not path.is_dir():
                missing.append(str(path))
        return missing
