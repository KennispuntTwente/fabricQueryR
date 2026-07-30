from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from fabricqueryr_sandbox.delta_oracle import (
    _write_local_fixtures,
    main,
    redact_error,
    read_delta_snapshot,
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
    evolved_zero = read_delta_table(
        str(tmp_path / "schema_evolved"),
        version=0,
    )
    nested = read_delta_table(str(tmp_path / "nested"))
    scalars = read_delta_table(str(tmp_path / "scalar_boundaries"))
    mutated, mutated_metadata = read_delta_snapshot(str(tmp_path / "mutated"))
    mutated_zero = read_delta_table(str(tmp_path / "mutated"), version=0)

    assert set(cases) == {
        "primitive_latest",
        "primitive_version_0",
        "primitive_projection",
        "empty",
        "schema_evolved",
        "schema_evolved_version_0",
        "nested",
        "scalar_boundaries",
        "mutated_latest",
        "mutated_version_0",
    }
    assert all(
        {
            "expected_rows",
            "expected_version",
            "expected_columns",
        }.issubset(case)
        for case in cases.values()
    )
    assert latest.num_rows == 5
    assert version_zero.num_rows == 2
    assert projected.column_names == ["name", "id", "amount"]
    assert projected.schema.field("amount").type == pa.string()
    assert b"" in latest.column("payload").to_pylist()
    assert "café / 数据=100%" in latest.column("category").to_pylist()
    assert empty.num_rows == 0
    assert empty.column_names == latest.column_names
    assert evolved.column_names == ["id", "name", "evolved_value"]
    assert evolved_zero.column_names == ["id", "name"]
    assert nested.num_rows == 3
    assert (
        nested.schema.field("profile").type.field("amount").type
        == pa.string()
    )
    assert (
        nested.schema.field("items").type.value_type.field("amount").type
        == pa.string()
    )
    assert (
        nested.schema.field("attributes").type.item_type.field("amount").type
        == pa.string()
    )
    nested_items = nested.column("items").combine_chunks()
    assert nested_items.values.null_count == 1
    assert nested_items.values[2].as_py() is None
    assert nested_items.values[3].as_py() == {
        "label": None,
        "amount": None,
        "code": None,
    }
    nested_attributes = nested.column("attributes").combine_chunks()
    assert nested_attributes.items.null_count == 1
    assert nested_attributes.items[1].as_py() is None
    assert nested_attributes.items[2].as_py() == {
        "enabled": None,
        "amount": None,
    }
    assert scalars.num_rows == 3
    assert scalars.schema.field("whole_decimal").type == pa.string()
    assert scalars.schema.field("scaled_decimal").type == pa.string()
    assert sorted(
        zip(
            mutated.column("id").to_pylist(),
            mutated.column("label").to_pylist(),
            strict=True,
        )
    ) == [
        (1, "one"),
        (3, "three-updated"),
        (4, "four"),
    ]
    assert sorted(mutated_zero.column("id").to_pylist()) == [1, 2, 3]
    assert mutated_metadata["version"] == 3
    assert mutated_metadata["active_file_count"] >= 1
    assert mutated_metadata["row_count"] == 3

    ipc_path = tmp_path / "oracle.arrow"
    write_ipc(projected, ipc_path)
    with ipc_path.open("rb") as stream:
        restored = ipc.open_file(stream).read_all()
    assert restored.equals(projected)

    metadata_path = tmp_path / "oracle.json"
    cli_path = tmp_path / "oracle-cli.arrow"
    assert (
        main(
            [
                "read",
                "--uri",
                str(tmp_path / "primitive"),
                "--output",
                str(cli_path),
                "--metadata-output",
                str(metadata_path),
                "--version",
                "0",
                "--column",
                "id",
            ]
        )
        == 0
    )
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["version"] == 0
    assert metadata["row_count"] == 2
    assert metadata["column_names"] == ["id"]
    assert metadata["reader_features"] == ["timestampNtz"]
    assert metadata["partition_columns"] == ["category"]
    assert metadata["configuration"] == {}
