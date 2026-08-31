from datetime import UTC, datetime, timedelta

from fabricqueryr_sandbox.cleanup import (
    cleanup_ci_workspaces,
    parse_ci_description,
    parse_persistent_description,
    remove_persistent_workspace,
)


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
            None,
            {
                "id": "null-description-id",
                "displayName": "fabricqueryr-ci-null-description",
                "description": None,
                "type": "Workspace",
            },
            {
                "id": "null-name-id",
                "displayName": None,
                "description": (
                    "fabricqueryr-ci; repo=owner/fabricQueryR; "
                    "created=2026-07-25T00:00:00Z; "
                    "run=https://example.test/null-name"
                ),
                "type": "Workspace",
            },
            {
                "id": None,
                "displayName": "fabricqueryr-ci-null-id",
                "description": (
                    "fabricqueryr-ci; repo=owner/fabricQueryR; "
                    "created=2026-07-25T00:00:00Z; "
                    "run=https://example.test/null-id"
                ),
                "type": "Workspace",
            },
            {
                "id": "sandbox-id",
                "displayName": "fabricqueryr-ci-123-1",
                "description": (
                    "fabricqueryr-ci; repo=owner/fabricQueryR; "
                    "created=2026-07-25T00:00:00Z; "
                    "run=https://example.test/123"
                ),
                "type": "Workspace",
            },
            {
                "id": "fresh-id",
                "displayName": "fabricqueryr-ci-124-1",
                "description": (
                    "fabricqueryr-ci; repo=owner/fabricQueryR; "
                    "created=2026-07-26T11:00:00Z; "
                    "run=https://example.test/124"
                ),
                "type": "Workspace",
            },
            {
                "id": "foreign-id",
                "displayName": "fabricqueryr-ci-125-1",
                "description": (
                    "fabricqueryr-ci; repo=other/fabricQueryR; "
                    "created=2026-07-25T00:00:00Z; "
                    "run=https://example.test/125"
                ),
                "type": "Workspace",
            },
            {
                "id": "legacy-id",
                "displayName": "fabricqueryr-ci-126-1",
                "description": "fabricqueryr-ci; run=https://example.test/126",
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
            {
                "id": "persistent-null-description-id",
                "displayName": "fabricqueryr-dev-dhrkoning",
                "description": None,
                "type": "Workspace",
            },
            {
                "id": None,
                "displayName": "fabricqueryr-dev-dhrkoning",
                "description": (
                    "fabricqueryr-persistent; repo=owner/fabricQueryR; "
                    "owner=user-id; managed-by=.github/workflows/"
                    "fabric-sandbox.yaml; rebuilt=2026-07-26T11:00:00Z; "
                    "run=https://example.test/null-id"
                ),
                "type": "Workspace",
            },
            {
                "id": "persistent-id",
                "displayName": "fabricqueryr-dev-dhrkoning",
                "description": (
                    "fabricqueryr-persistent; repo=owner/fabricQueryR; "
                    "owner=user-id; managed-by=.github/workflows/"
                    "fabric-sandbox.yaml; rebuilt=2026-07-26T11:00:00Z; "
                    "run=https://example.test/persistent"
                ),
                "type": "Workspace",
            },
            {
                "id": "persistent-foreign-id",
                "displayName": "fabricqueryr-dev-dhrkoning",
                "description": (
                    "fabricqueryr-persistent; repo=other/fabricQueryR; "
                    "owner=user-id; managed-by=.github/workflows/"
                    "fabric-sandbox.yaml; rebuilt=2026-07-26T11:00:00Z; "
                    "run=https://example.test/persistent"
                ),
                "type": "Workspace",
            },
        ]

    def delete_workspace(self, workspace_id):
        type(self).deleted.append(workspace_id)


def test_marker_parsers_reject_non_string_descriptions():
    for value in (None, {}, []):
        assert parse_ci_description(value) is None
        assert parse_persistent_description(value) is None


def test_cleanup_is_dry_run_by_default(monkeypatch):
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.FabricApi", FakeFabricApi
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.get_credential", lambda: "credential"
    )

    candidates = cleanup_ci_workspaces(
        repository="owner/fabricQueryR",
        now=datetime(2026, 7, 26, 12, tzinfo=UTC),
    )

    assert [workspace["id"] for workspace in candidates] == ["sandbox-id"]
    assert FakeFabricApi.deleted == []


def test_cleanup_deletes_only_double_marked_ci_workspaces(monkeypatch):
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.FabricApi", FakeFabricApi
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.get_credential", lambda: "credential"
    )

    cleanup_ci_workspaces(
        confirm=True,
        repository="OWNER/FABRICQUERYR",
        now=datetime(2026, 7, 26, 12, tzinfo=UTC),
        minimum_age=timedelta(hours=6),
    )

    assert FakeFabricApi.deleted == ["sandbox-id"]


def test_cleanup_requires_repository_identity(monkeypatch):
    monkeypatch.delenv("GITHUB_REPOSITORY", raising=False)

    try:
        cleanup_ci_workspaces()
    except RuntimeError as error:
        assert "Repository identity" in str(error)
    else:
        raise AssertionError("cleanup accepted an unidentified repository")


def test_cleanup_rejects_negative_minimum_age():
    try:
        cleanup_ci_workspaces(
            repository="owner/fabricQueryR",
            minimum_age=timedelta(hours=-1),
        )
    except ValueError as error:
        assert "minimum_age" in str(error)
    else:
        raise AssertionError("cleanup accepted a negative minimum age")


def test_persistent_cleanup_is_dry_run_by_default(monkeypatch):
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.FabricApi", FakeFabricApi
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.get_credential", lambda: "credential"
    )

    candidates = remove_persistent_workspace(
        workspace_name="fabricqueryr-dev-dhrkoning",
        owner_id="user-id",
        managed_by=".github/workflows/fabric-sandbox.yaml",
        repository="OWNER/FABRICQUERYR",
    )

    assert [workspace["id"] for workspace in candidates] == ["persistent-id"]
    assert FakeFabricApi.deleted == []


def test_persistent_cleanup_deletes_only_exact_owned_workspace(monkeypatch):
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.FabricApi", FakeFabricApi
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.get_credential", lambda: "credential"
    )

    remove_persistent_workspace(
        workspace_name="fabricqueryr-dev-dhrkoning",
        owner_id="user-id",
        managed_by=".github/workflows/fabric-sandbox.yaml",
        confirm=True,
        repository="owner/fabricQueryR",
    )

    assert FakeFabricApi.deleted == ["persistent-id"]


def test_persistent_cleanup_rejects_empty_workspace_name():
    try:
        remove_persistent_workspace(
            workspace_name=" ",
            owner_id="user-id",
            managed_by=".github/workflows/fabric-sandbox.yaml",
            repository="owner/fabricQueryR",
        )
    except ValueError as error:
        assert "workspace_name" in str(error)
    else:
        raise AssertionError("persistent cleanup accepted an empty name")


def test_persistent_cleanup_requires_exact_marker_owner(monkeypatch):
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.FabricApi", FakeFabricApi
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cleanup.get_credential", lambda: "credential"
    )

    candidates = remove_persistent_workspace(
        workspace_name="fabricqueryr-dev-dhrkoning",
        owner_id="another-user-id",
        managed_by=".github/workflows/fabric-sandbox.yaml",
        repository="owner/fabricQueryR",
    )

    assert candidates == []
