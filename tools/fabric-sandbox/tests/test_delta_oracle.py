from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from fabricqueryr_sandbox.delta_oracle import (
    _write_local_fixtures,
    redact_error,
    read_delta_table,
    storage_options,
    write_ipc,
)


def test_storage_options_keep_onelake_tokens_in_the_environment(monkeypatch):
    monkeypatch.setenv("FABRIC_TEST_STORAGE_TOKEN", "secret-token")

    assert storage_options(
        "abfss://workspace@onelake.dfs.fabric.microsoft.com/item/Tables/table"
    ) == {
        "bearer_token": "secret-token",
        "use_fabric_endpoint": "true",
    }
    assert storage_options("/tmp/local-delta") is None


def test_storage_options_require_a_token_for_abfs(monkeypatch):
    monkeypatch.delenv("FABRIC_TEST_STORAGE_TOKEN", raising=False)

    with pytest.raises(RuntimeError, match="FABRIC_TEST_STORAGE_TOKEN"):
        storage_options(
            "abfss://workspace@onelake.dfs.fabric.microsoft.com/"
            "item/Tables/table"
        )


def test_oracle_errors_redact_the_storage_token(monkeypatch):
    monkeypatch.setenv("FABRIC_TEST_STORAGE_TOKEN", "secret-token")

    assert redact_error("request failed for secret-token") == (
        "request failed for <redacted>"
    )


def test_local_fixture_oracle_covers_versions_projection_and_empty_schema(
    tmp_path: Path,
):
    manifest = _write_local_fixtures(tmp_path)
    cases = {case["name"]: case for case in manifest["cases"]}

    latest = read_delta_table(str(tmp_path / "primitive"))
    version_zero = read_delta_table(str(tmp_path / "primitive"), version=0)
    projected = read_delta_table(
        str(tmp_path / "primitive"),
        columns=["name", "id", "amount"],
    )
    empty = read_delta_table(str(tmp_path / "empty"))
    evolved = read_delta_table(str(tmp_path / "schema_evolved"))
    nested = read_delta_table(str(tmp_path / "nested"))

    assert set(cases) == {
        "primitive_latest",
        "primitive_version_0",
        "primitive_projection",
        "empty",
        "schema_evolved",
        "nested",
    }
    assert latest.num_rows == 4
    assert version_zero.num_rows == 2
    assert projected.column_names == ["name", "id", "amount"]
    assert projected.schema.field("amount").type == pa.string()
    assert b"" in latest.column("payload").to_pylist()
    assert empty.num_rows == 0
    assert empty.column_names == latest.column_names
    assert evolved.column_names == ["id", "name", "evolved_value"]
    assert nested.num_rows == 2
    assert (
        nested.schema.field("profile").type.field("amount").type
        == pa.string()
    )

    ipc_path = tmp_path / "oracle.arrow"
    write_ipc(projected, ipc_path)
    with ipc_path.open("rb") as stream:
        restored = ipc.open_file(stream).read_all()
    assert restored.equals(projected)
