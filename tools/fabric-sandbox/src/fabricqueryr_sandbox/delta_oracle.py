"""delta-rs test oracle for the R Delta reader.

This module is intentionally test-only. It is not part of the R package and
never participates in the package's runtime implementation.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import sys
from typing import Any, Sequence

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.ipc as ipc


STORAGE_TOKEN_VARIABLE = "FABRIC_TEST_STORAGE_TOKEN"


def redact_error(message: str) -> str:
    """Remove the OneLake bearer token from errors captured by test runners."""
    token = os.environ.get(STORAGE_TOKEN_VARIABLE, "")
    if token:
        return message.replace(token, "<redacted>")
    return message


def storage_options(table_uri: str) -> dict[str, str] | None:
    """Return OneLake delta-rs options without putting tokens on the CLI."""
    if not table_uri.lower().startswith(("abfs://", "abfss://")):
        return None
    token = os.environ.get(STORAGE_TOKEN_VARIABLE, "")
    if not token:
        raise RuntimeError(
            f"{STORAGE_TOKEN_VARIABLE} is required for an ABFS table"
        )
    return {
        "bearer_token": token,
        "use_fabric_endpoint": "true",
    }


def read_delta_table(
    table_uri: str,
    *,
    version: int | None = None,
    columns: Sequence[str] | None = None,
    limit: int | None = None,
) -> pa.Table:
    """Read one Delta snapshot with the independent delta-rs Python binding."""
    table, _ = read_delta_snapshot(
        table_uri,
        version=version,
        columns=columns,
        limit=limit,
    )
    return table


def read_delta_snapshot(
    table_uri: str,
    *,
    version: int | None = None,
    columns: Sequence[str] | None = None,
    limit: int | None = None,
) -> tuple[pa.Table, dict[str, Any]]:
    """Read rows plus independent snapshot metadata used by parity assertions."""
    delta = DeltaTable(
        table_uri,
        version=version,
        storage_options=storage_options(table_uri),
    )
    table = delta.to_pyarrow_table(
        columns=list(columns) if columns else None
    )
    if limit is not None:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        table = table.slice(0, limit)
    table = normalize_exact_decimals(table)
    protocol = delta.protocol()
    delta_metadata = delta.metadata()
    metadata = {
        "version": delta.version(),
        "min_reader_version": protocol.min_reader_version,
        "min_writer_version": protocol.min_writer_version,
        "reader_features": sorted(protocol.reader_features or []),
        "writer_features": sorted(protocol.writer_features or []),
        "active_file_count": len(delta.file_uris()),
        "row_count": table.num_rows,
        "column_names": table.column_names,
        "partition_columns": delta_metadata.partition_columns,
        "configuration": delta_metadata.configuration,
        "delta_schema": json.loads(delta.schema().to_json()),
        "arrow_schema": str(table.schema),
    }
    return table, metadata


def _normalize_array(array: pa.Array) -> pa.Array:
    data_type = array.type
    if pa.types.is_decimal(data_type):
        return pc.cast(array, pa.string())
    if pa.types.is_timestamp(data_type) and data_type.tz is None:
        # R's POSIXct has instant semantics and binary-double precision. Keep
        # Delta timestamp_ntz values as exact wall-clock text for comparison.
        return pc.cast(array, pa.string())
    if pa.types.is_struct(data_type):
        children = [
            _normalize_array(array.field(index))
            for index in range(data_type.num_fields)
        ]
        fields = [
            data_type[index].with_type(children[index].type)
            for index in range(data_type.num_fields)
        ]
        return pa.StructArray.from_arrays(
            children,
            fields=fields,
            mask=array.is_null(),
        )
    if pa.types.is_list(data_type):
        return pa.ListArray.from_arrays(
            array.offsets,
            _normalize_array(array.values),
            mask=array.is_null(),
        )
    if pa.types.is_large_list(data_type):
        return pa.LargeListArray.from_arrays(
            array.offsets,
            _normalize_array(array.values),
            mask=array.is_null(),
        )
    if pa.types.is_fixed_size_list(data_type):
        return pa.FixedSizeListArray.from_arrays(
            _normalize_array(array.values),
            data_type.list_size,
            mask=array.is_null(),
        )
    if pa.types.is_map(data_type):
        return pa.MapArray.from_arrays(
            array.offsets,
            _normalize_array(array.keys),
            _normalize_array(array.items),
            mask=array.is_null(),
        )
    return array


def normalize_exact_decimals(table: pa.Table) -> pa.Table:
    """Convert decimals recursively to exact strings for the R comparison."""
    arrays = []
    fields = []
    for field, column in zip(table.schema, table.columns, strict=True):
        array = _normalize_array(column.combine_chunks())
        arrays.append(array)
        fields.append(field.with_type(array.type))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def write_ipc(table: pa.Table, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as stream:
        with ipc.new_file(stream, table.schema) as writer:
            writer.write_table(table)


def _primitive_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("name", pa.string()),
            pa.field("category", pa.string()),
            pa.field("amount", pa.decimal128(20, 4)),
            pa.field("active", pa.bool_()),
            pa.field("event_date", pa.date32()),
            pa.field("observed_at", pa.timestamp("us", tz="UTC")),
            pa.field("local_at", pa.timestamp("us")),
            pa.field("payload", pa.binary()),
        ]
    )


def _primitive_table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows, schema=_primitive_schema())


def _write_local_fixtures(directory: Path) -> dict[str, Any]:
    directory.mkdir(parents=True, exist_ok=True)

    primitive = directory / "primitive"
    write_deltalake(
        primitive,
        _primitive_table(
            [
                {
                    "id": 1,
                    "name": "alpha",
                    "category": "A",
                    "amount": Decimal("12.3000"),
                    "active": True,
                    "event_date": date(2026, 1, 1),
                    "observed_at": datetime(
                        2026, 1, 1, 12, 34, 56, 123456, tzinfo=timezone.utc
                    ),
                    "local_at": datetime(2026, 7, 28, 9, 8, 7, 654321),
                    "payload": b"\x00\xff\x10",
                },
                {
                    "id": 9_007_199_254_740_993,
                    "name": "café-数据-🙂",
                    "category": "B",
                    "amount": Decimal("-0.5000"),
                    "active": False,
                    "event_date": date(1969, 12, 31),
                    "observed_at": datetime(
                        1969, 12, 31, 23, 59, 59, 1, tzinfo=timezone.utc
                    ),
                    "local_at": datetime(1900, 1, 1, 0, 0, 0, 1),
                    "payload": b"\x7f",
                },
            ]
        ),
        mode="overwrite",
        partition_by="category",
    )
    write_deltalake(
        primitive,
        _primitive_table(
            [
                {
                    "id": 3,
                    "name": None,
                    "category": None,
                    "amount": None,
                    "active": None,
                    "event_date": None,
                    "observed_at": None,
                    "local_at": None,
                    "payload": None,
                },
                {
                    "id": 4,
                    "name": "empty binary",
                    "category": "C",
                    "amount": Decimal("0.0000"),
                    "active": True,
                    "event_date": date(2000, 2, 29),
                    "observed_at": datetime(
                        2000, 2, 29, tzinfo=timezone.utc
                    ),
                    "local_at": datetime(2000, 2, 29),
                    "payload": b"",
                },
                {
                    "id": 5,
                    "name": "encoded partition",
                    "category": "café / 数据=100%",
                    "amount": Decimal("1.0000"),
                    "active": True,
                    "event_date": date(2038, 1, 19),
                    "observed_at": datetime(
                        2038, 1, 19, 3, 14, 7, 999999, tzinfo=timezone.utc
                    ),
                    "local_at": datetime(2038, 1, 19, 3, 14, 7, 999999),
                    "payload": b"\xc3\xa9",
                },
            ]
        ),
        mode="append",
    )

    empty = directory / "empty"
    write_deltalake(
        empty,
        pa.Table.from_batches([], schema=_primitive_schema()),
        mode="overwrite",
        partition_by="category",
    )

    evolved = directory / "schema_evolved"
    write_deltalake(
        evolved,
        pa.table({"id": pa.array([1, 2], pa.int32()), "name": ["one", "two"]}),
        mode="overwrite",
    )
    write_deltalake(
        evolved,
        pa.table(
            {
                "id": pa.array([3], pa.int32()),
                "name": ["three"],
                "evolved_value": ["introduced"],
            }
        ),
        mode="append",
        schema_mode="merge",
    )

    nested = directory / "nested"
    nested_schema = pa.schema(
        [
            pa.field("id", pa.int32()),
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field("label", pa.string()),
                        pa.field("amount", pa.decimal128(20, 2)),
                    ]
                ),
            ),
            pa.field("scores", pa.list_(pa.int32())),
            pa.field("counts", pa.map_(pa.string(), pa.int64())),
            pa.field(
                "items",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("label", pa.string()),
                            pa.field("amount", pa.decimal128(20, 3)),
                            pa.field("code", pa.int64()),
                        ]
                    )
                ),
            ),
            pa.field(
                "attributes",
                pa.map_(
                    pa.string(),
                    pa.struct(
                        [
                            pa.field("enabled", pa.bool_()),
                            pa.field("amount", pa.decimal128(20, 2)),
                        ]
                    ),
                ),
            ),
            pa.field(
                "keyed",
                pa.map_(
                    pa.struct(
                        [
                            pa.field("marker", pa.int32()),
                            pa.field(
                                "nested",
                                pa.struct(
                                    [
                                        pa.field("number", pa.int32()),
                                        pa.field("text", pa.string()),
                                    ]
                                ),
                            ),
                        ]
                    ),
                    pa.string(),
                ),
            ),
        ]
    )
    write_deltalake(
        nested,
        pa.Table.from_pylist(
            [
                {
                    "id": 1,
                    "profile": {
                        "label": "nested",
                        "amount": Decimal("123456789012345678.90"),
                    },
                    "scores": [1, 2, 3],
                    "counts": [("large", 9_007_199_254_740_993), ("small", 2)],
                    "items": [
                        {
                            "label": "first",
                            "amount": Decimal("1.250"),
                            "code": 9_007_199_254_740_993,
                        },
                        {
                            "label": "second",
                            "amount": None,
                            "code": 2,
                        },
                        None,
                        {
                            "label": None,
                            "amount": None,
                            "code": None,
                        },
                    ],
                    "attributes": [
                        (
                            "primary",
                            {
                                "enabled": True,
                                "amount": Decimal("99.99"),
                            },
                        ),
                        ("null", None),
                        (
                            "all-null",
                            {
                                "enabled": None,
                                "amount": None,
                            },
                        ),
                    ],
                    "keyed": [
                        ({"marker": 1, "nested": None}, "null"),
                        (
                            {
                                "marker": 2,
                                "nested": {"number": None, "text": None},
                            },
                            "present",
                        ),
                    ],
                },
                {
                    "id": 2,
                    "profile": None,
                    "scores": [],
                    "counts": [],
                    "items": [],
                    "attributes": [],
                    "keyed": [],
                },
                {
                    "id": 3,
                    "profile": {
                        "label": None,
                        "amount": None,
                    },
                    "scores": None,
                    "counts": None,
                    "items": None,
                    "attributes": None,
                    "keyed": None,
                },
            ],
            schema=nested_schema,
        ),
        mode="overwrite",
    )

    scalars = directory / "scalar_boundaries"
    scalar_schema = pa.schema(
        [
            pa.field("row_id", pa.int32()),
            pa.field("tiny", pa.int8()),
            pa.field("small", pa.int16()),
            pa.field("regular", pa.int32()),
            pa.field("large", pa.int64()),
            pa.field("single", pa.float32()),
            pa.field("double", pa.float64()),
            pa.field("whole_decimal", pa.decimal128(38, 0)),
            pa.field("scaled_decimal", pa.decimal128(38, 18)),
            pa.field("text", pa.string()),
            pa.field("payload", pa.binary()),
            pa.field("event_date", pa.date32()),
            pa.field("observed_at", pa.timestamp("us", tz="UTC")),
            pa.field("local_at", pa.timestamp("us")),
            pa.field("active", pa.bool_()),
        ]
    )
    write_deltalake(
        scalars,
        pa.Table.from_pylist(
            [
                {
                    "row_id": 1,
                    "tiny": -128,
                    "small": -32768,
                    "regular": -2147483648,
                    "large": -9_223_372_036_854_775_807,
                    "single": float("nan"),
                    "double": float("inf"),
                    "whole_decimal": Decimal(
                        "-99999999999999999999999999999999999999"
                    ),
                    "scaled_decimal": Decimal(
                        "-99999999999999999999.999999999999999999"
                    ),
                    "text": "",
                    "payload": b"",
                    "event_date": date(1900, 1, 1),
                    "observed_at": datetime(
                        1900, 1, 1, 0, 0, 0, 1, tzinfo=timezone.utc
                    ),
                    "local_at": datetime(1900, 1, 1, 0, 0, 0, 1),
                    "active": False,
                },
                {
                    "row_id": 2,
                    "tiny": 127,
                    "small": 32767,
                    "regular": 2147483647,
                    "large": 9_223_372_036_854_775_807,
                    "single": float("-inf"),
                    "double": -0.0,
                    "whole_decimal": Decimal(
                        "99999999999999999999999999999999999999"
                    ),
                    "scaled_decimal": Decimal(
                        "99999999999999999999.999999999999999999"
                    ),
                    "text": "café-数据-🙂",
                    "payload": b"\x00\xff",
                    "event_date": date(2038, 1, 19),
                    "observed_at": datetime(
                        2038, 1, 19, 3, 14, 7, 999999, tzinfo=timezone.utc
                    ),
                    "local_at": datetime(2038, 1, 19, 3, 14, 7, 999999),
                    "active": True,
                },
                {
                    "row_id": 3,
                    "tiny": None,
                    "small": None,
                    "regular": None,
                    "large": None,
                    "single": None,
                    "double": float("nan"),
                    "whole_decimal": None,
                    "scaled_decimal": None,
                    "text": None,
                    "payload": None,
                    "event_date": None,
                    "observed_at": None,
                    "local_at": None,
                    "active": None,
                },
            ],
            schema=scalar_schema,
        ),
        mode="overwrite",
    )

    mutated = directory / "mutated"
    write_deltalake(
        mutated,
        pa.table(
            {
                "id": pa.array([1, 2, 3], pa.int64()),
                "label": ["one", "two", "three"],
            }
        ),
        mode="overwrite",
    )
    DeltaTable(str(mutated)).delete("id = 2")
    DeltaTable(str(mutated)).update(
        updates={"label": "'three-updated'"},
        predicate="id = 3",
    )
    write_deltalake(
        mutated,
        pa.table(
            {
                "id": pa.array([4], pa.int64()),
                "label": ["four"],
            }
        ),
        mode="append",
    )

    cases = [
        {
            "name": "primitive_latest",
            "table": "primitive",
            "expected_rows": 5,
            "expected_version": 1,
            "expected_columns": [field.name for field in _primitive_schema()],
        },
        {
            "name": "primitive_version_0",
            "table": "primitive",
            "version": 0,
            "expected_rows": 2,
            "expected_version": 0,
            "expected_columns": [field.name for field in _primitive_schema()],
        },
        {
            "name": "primitive_projection",
            "table": "primitive",
            "columns": ["name", "id", "amount"],
            "expected_rows": 5,
            "expected_version": 1,
            "expected_columns": ["name", "id", "amount"],
        },
        {
            "name": "primitive_limit_zero",
            "table": "primitive",
            "limit": 0,
            "expected_rows": 0,
            "expected_version": 1,
            "expected_columns": [field.name for field in _primitive_schema()],
        },
        {
            "name": "primitive_limit_all",
            "table": "primitive",
            "limit": 5,
            "expected_rows": 5,
            "expected_version": 1,
            "expected_columns": [field.name for field in _primitive_schema()],
        },
        {
            # A partial limit selects an implementation-defined subset of rows,
            # so compare it as a sub-multiset of the full snapshot instead of
            # requiring both readers to pick the same rows.
            "name": "primitive_limit_partial",
            "table": "primitive",
            "limit": 2,
            "comparison": "subset",
            "expected_rows": 2,
            "expected_version": 1,
            "expected_columns": [field.name for field in _primitive_schema()],
        },
        {
            "name": "primitive_projection_limit",
            "table": "primitive",
            "columns": ["name", "id", "amount"],
            "limit": 3,
            "comparison": "subset",
            "expected_rows": 3,
            "expected_version": 1,
            "expected_columns": ["name", "id", "amount"],
        },
        {
            "name": "nested_limit_partial",
            "table": "nested",
            "limit": 2,
            "comparison": "subset",
            "expected_rows": 2,
            "expected_version": 0,
            "expected_columns": [
                "id",
                "profile",
                "scores",
                "counts",
                "items",
                "attributes",
                "keyed",
            ],
        },
        {
            "name": "mutated_limit_partial",
            "table": "mutated",
            "limit": 1,
            "comparison": "subset",
            "expected_rows": 1,
            "expected_version": 3,
            "expected_columns": ["id", "label"],
        },
        {
            "name": "empty",
            "table": "empty",
            "expected_rows": 0,
            "expected_version": 0,
            "expected_columns": [field.name for field in _primitive_schema()],
        },
        {
            "name": "empty_limit",
            "table": "empty",
            "limit": 4,
            "expected_rows": 0,
            "expected_version": 0,
            "expected_columns": [field.name for field in _primitive_schema()],
        },
        {
            "name": "schema_evolved",
            "table": "schema_evolved",
            "expected_rows": 3,
            "expected_version": 1,
            "expected_columns": ["id", "name", "evolved_value"],
        },
        {
            "name": "schema_evolved_version_0",
            "table": "schema_evolved",
            "version": 0,
            "expected_rows": 2,
            "expected_version": 0,
            "expected_columns": ["id", "name"],
        },
        {
            "name": "nested",
            "table": "nested",
            "expected_rows": 3,
            "expected_version": 0,
            "expected_columns": [
                "id",
                "profile",
                "scores",
                "counts",
                "items",
                "attributes",
                "keyed",
            ],
        },
        {
            "name": "scalar_boundaries",
            "table": "scalar_boundaries",
            "expected_rows": 3,
            "expected_version": 0,
            "expected_columns": [field.name for field in scalar_schema],
        },
        {
            "name": "mutated_latest",
            "table": "mutated",
            "expected_rows": 3,
            "expected_version": 3,
            "expected_columns": ["id", "label"],
        },
        {
            "name": "mutated_version_0",
            "table": "mutated",
            "version": 0,
            "expected_rows": 3,
            "expected_version": 0,
            "expected_columns": ["id", "label"],
        },
    ]
    manifest = {
        "deltalake_version": __import__("deltalake").__version__,
        "pyarrow_version": pa.__version__,
        "cases": cases,
    }
    (directory / "manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    read = commands.add_parser("read", help="read a Delta snapshot to Arrow IPC")
    read.add_argument("--uri", required=True)
    read.add_argument("--output", type=Path, required=True)
    read.add_argument("--metadata-output", type=Path)
    read.add_argument("--version", type=int)
    read.add_argument("--column", action="append", dest="columns")
    read.add_argument("--limit", type=int)

    fixtures = commands.add_parser(
        "write-fixtures",
        help="write deterministic local Delta tables for cross-language tests",
    )
    fixtures.add_argument("--directory", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "read":
        table, metadata = read_delta_snapshot(
            args.uri,
            version=args.version,
            columns=args.columns,
            limit=args.limit,
        )
        write_ipc(
            table,
            args.output,
        )
        if args.metadata_output is not None:
            args.metadata_output.parent.mkdir(parents=True, exist_ok=True)
            args.metadata_output.write_text(
                json.dumps(metadata, indent=2) + "\n",
                encoding="utf-8",
            )
        return 0
    if args.command == "write-fixtures":
        _write_local_fixtures(args.directory)
        return 0
    raise AssertionError(f"unexpected command: {args.command}")


def cli(argv: Sequence[str] | None = None) -> int:
    try:
        return main(argv)
    except Exception as error:
        print(
            f"delta-rs oracle failed: {redact_error(str(error))}",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(cli())
