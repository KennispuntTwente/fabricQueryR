"""Serializable contract between sandbox provisioning and R integration tests."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class SandboxManifest:
    workspace_id: str
    workspace_name: str
    items: dict[str, dict[str, Any]] = field(default_factory=dict)

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(asdict(self), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    @classmethod
    def read(cls, path: Path) -> "SandboxManifest":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return cls(**payload)