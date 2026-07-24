from fabricqueryr_sandbox.seed import upload_fixtures
from fabricqueryr_sandbox.settings import SandboxSettings


class FakeFileClient:
    def __init__(self, path, uploaded):
        self.path = path
        self.uploaded = uploaded

    def upload_data(self, source, *, overwrite):
        self.uploaded[self.path] = (source.read(), overwrite)


class FakeFileSystem:
    def __init__(self, uploaded):
        self.uploaded = uploaded

    def get_file_client(self, path):
        return FakeFileClient(path, self.uploaded)


class FakeDataLakeServiceClient:
    uploaded = {}
    account_url = None
    credential = None
    filesystem = None

    def __init__(self, *, account_url, credential):
        type(self).account_url = account_url
        type(self).credential = credential

    def get_file_system_client(self, filesystem):
        type(self).filesystem = filesystem
        return FakeFileSystem(type(self).uploaded)


def test_upload_fixtures_includes_livy_batch_application(monkeypatch, tmp_path):
    fixture_dir = tmp_path / "infra/fabric/fixtures"
    fixture_dir.mkdir(parents=True)
    (fixture_dir / "basic.csv").write_text("id,name\n1,alpha\n")
    (fixture_dir / "livy_batch.py").write_text("print('batch')\n")
    settings = SandboxSettings(
        workspace_id="workspace-id",
        lakehouse_id="lakehouse-id",
        workspace_name="fabricqueryr-test",
        capacity_id=None,
        principal_id=None,
        environment="TEST",
        repository_root=tmp_path,
        manifest_path=tmp_path / "manifest.json",
    )
    FakeDataLakeServiceClient.uploaded = {}
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.DataLakeServiceClient",
        FakeDataLakeServiceClient,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.seed.get_credential",
        lambda: "credential",
    )

    upload_fixtures(settings, "workspace-id", "lakehouse-id")

    assert FakeDataLakeServiceClient.account_url == (
        "https://onelake.dfs.fabric.microsoft.com"
    )
    assert FakeDataLakeServiceClient.filesystem == "workspace-id"
    assert set(FakeDataLakeServiceClient.uploaded) == {
        "lakehouse-id/Files/fixtures/basic.csv",
        "lakehouse-id/Files/fixtures/livy_batch.py",
    }
    batch = FakeDataLakeServiceClient.uploaded[
        "lakehouse-id/Files/fixtures/livy_batch.py"
    ]
    assert batch[0].replace(b"\r\n", b"\n") == b"print('batch')\n"
    assert batch[1] is True
