from fabricqueryr_sandbox import SandboxManifest


def test_manifest_round_trip(tmp_path):
    path = tmp_path / "manifest.json"
    manifest = SandboxManifest(
        workspace_id="workspace-id",
        workspace_name="fabricqueryr-test",
        items={"TestLakehouse": {"id": "lakehouse-id", "type": "Lakehouse"}},
    )

    manifest.write(path)

    assert SandboxManifest.read(path) == manifest