from fabricqueryr_sandbox.cleanup import cleanup_ci_workspaces


class FakeFabricApi:
    deleted = []

    def __init__(self, _credential):
        type(self).deleted = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def list_workspaces(self, *, roles):
        assert roles == "Admin"
        return [
            {
                "id": "sandbox-id",
                "displayName": "fabricqueryr-ci-123-1",
                "description": "fabricqueryr-ci; run=https://example.test/123",
                "type": "Workspace",
            },
            {
                "id": "similar-name-id",
                "displayName": "fabricqueryr-ci-important",
                "description": "production workspace",
                "type": "Workspace",
            },
            {
                "id": "similar-description-id",
                "displayName": "fabricqueryr-development",
                "description": "fabricqueryr-ci; run=manual",
                "type": "Workspace",
            },
            {
                "id": "personal-id",
                "displayName": "fabricqueryr-ci-personal",
                "description": "fabricqueryr-ci; run=manual",
                "type": "Personal",
            },
        ]

    def delete_workspace(self, workspace_id):
        type(self).deleted.append(workspace_id)


def test_cleanup_is_dry_run_by_default(monkeypatch):
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.FabricApi", FakeFabricApi
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.get_credential", lambda: "credential"
    )

    candidates = cleanup_ci_workspaces()

    assert [workspace["id"] for workspace in candidates] == ["sandbox-id"]
    assert FakeFabricApi.deleted == []


def test_cleanup_deletes_only_double_marked_ci_workspaces(monkeypatch):
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.FabricApi", FakeFabricApi
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.get_credential", lambda: "credential"
    )

    cleanup_ci_workspaces(confirm=True)

    assert FakeFabricApi.deleted == ["sandbox-id"]
