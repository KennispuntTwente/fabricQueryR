import io
import json
from types import SimpleNamespace

import pyarrow.parquet as pq

from fabricqueryr_sandbox.open_mirroring import (
    MIRRORED_FIXTURE_TABLE,
    _next_data_file,
    upload_open_mirroring_fixture,
)


class FakeFileClient:
    def __init__(self, path, uploads):
        self.path = path
        self.uploads = uploads

    def upload_data(self, source, *, overwrite):
        self.uploads[self.path] = (source.read(), overwrite)


class FakeFileSystem:
    def __init__(self):
        self.directories = []
        self.uploads = {}
        self.listed = []

    def get_directory_client(self, path):
        filesystem = self

        class Directory:
            def create_directory(self):
                filesystem.directories.append(path)

        return Directory()

    def get_file_client(self, path):
        return FakeFileClient(path, self.uploads)

    def get_paths(self, *, path, recursive):
        self.listed.append((path, recursive))
        return [
            SimpleNamespace(
                name=f"{path}/00000000000000000002.parquet",
                is_directory=False,
            ),
            SimpleNamespace(name=f"{path}/_metadata.json", is_directory=False),
        ]


class FakeService:
    def __init__(self):
        self.filesystem = FakeFileSystem()
        self.workspace_id = None

    def get_file_system_client(self, workspace_id):
        self.workspace_id = workspace_id
        return self.filesystem


def test_next_data_file_ignores_non_data_paths():
    paths = [
        SimpleNamespace(name="table/_metadata.json", is_directory=False),
        SimpleNamespace(name="table/_ProcessedFiles", is_directory=True),
        SimpleNamespace(name="table/00000000000000000009.parquet", is_directory=False),
    ]

    assert _next_data_file(paths) == "00000000000000000010.parquet"


def test_open_mirroring_fixture_uploads_metadata_and_upsert_parquet():
    service = FakeService()

    remote_path = upload_open_mirroring_fixture(
        "workspace-id",
        "mirrored-id",
        service_client=service,
    )

    table_path = f"mirrored-id/Files/LandingZone/dbo.schema/{MIRRORED_FIXTURE_TABLE}"
    assert service.workspace_id == "workspace-id"
    assert service.filesystem.directories == [
        "mirrored-id/Files/LandingZone",
        "mirrored-id/Files/LandingZone/dbo.schema",
        table_path,
    ]
    assert service.filesystem.listed == [(table_path, False)]
    assert remote_path == f"{table_path}/00000000000000000003.parquet"

    metadata, metadata_overwrite = service.filesystem.uploads[
        f"{table_path}/_metadata.json"
    ]
    assert json.loads(metadata) == {"keyColumns": ["id"]}
    assert metadata_overwrite is True

    parquet, parquet_overwrite = service.filesystem.uploads[remote_path]
    table = pq.read_table(io.BytesIO(parquet))
    assert table.column_names == ["id", "name", "amount", "__rowMarker__"]
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert table.column("name").to_pylist() == ["alpha", "beta", "gamma"]
    assert table.column("amount").to_pylist() == [10.5, 20.0, None]
    assert table.column("__rowMarker__").to_pylist() == [4, 4, 4]
    assert parquet_overwrite is True
