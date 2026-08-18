"""Seed the Fabric open-mirroring landing zone used by integration tests."""

from __future__ import annotations

import io
import json
import re

from azure.core.credentials import TokenCredential
from azure.core.exceptions import ResourceExistsError
from azure.storage.filedatalake import DataLakeServiceClient
import pyarrow as pa
import pyarrow.parquet as pq

from .credentials import get_credential


MIRRORED_FIXTURE_SCHEMA = "dbo"
MIRRORED_FIXTURE_TABLE = "fabricqueryr_mirror_types"
_DATA_FILE_PATTERN = re.compile(r"^(\d{20})\.parquet$")


def _fixture_parquet() -> bytes:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "name": pa.array(["alpha", "beta", "gamma"], type=pa.string()),
            "amount": pa.array([10.5, 20.0, None], type=pa.float64()),
            "__rowMarker__": pa.array([4, 4, 4], type=pa.int32()),
        }
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    return sink.getvalue().to_pybytes()


def _next_data_file(paths: object) -> str:
    numbers = []
    for path in paths:
        if getattr(path, "is_directory", False):
            continue
        name = str(getattr(path, "name", "")).rsplit("/", 1)[-1]
        match = _DATA_FILE_PATTERN.fullmatch(name)
        if match:
            numbers.append(int(match.group(1)))
    next_number = max(numbers, default=0) + 1
    return f"{next_number:020}.parquet"


def upload_open_mirroring_fixture(
    workspace_id: str,
    mirrored_database_id: str,
    *,
    credential: TokenCredential | None = None,
    service_client: DataLakeServiceClient | None = None,
) -> str:
    """Publish an idempotent three-row upsert file to open mirroring."""
    service = service_client or DataLakeServiceClient(
        account_url="https://onelake.dfs.fabric.microsoft.com",
        credential=credential or get_credential(),
    )
    filesystem = service.get_file_system_client(workspace_id)
    landing_zone = f"{mirrored_database_id}/Files/LandingZone"
    schema_path = f"{landing_zone}/{MIRRORED_FIXTURE_SCHEMA}.schema"
    table_path = f"{schema_path}/{MIRRORED_FIXTURE_TABLE}"
    for directory in (landing_zone, schema_path, table_path):
        try:
            filesystem.get_directory_client(directory).create_directory()
        except ResourceExistsError:
            pass

    metadata = json.dumps(
        {"keyColumns": ["id"]},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    filesystem.get_file_client(f"{table_path}/_metadata.json").upload_data(
        io.BytesIO(metadata),
        overwrite=True,
    )
    data_file = _next_data_file(filesystem.get_paths(path=table_path, recursive=False))
    remote_path = f"{table_path}/{data_file}"
    filesystem.get_file_client(remote_path).upload_data(
        io.BytesIO(_fixture_parquet()),
        overwrite=True,
    )
    return remote_path
