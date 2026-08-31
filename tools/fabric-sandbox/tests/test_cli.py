import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from fabricqueryr_sandbox.cli import build_parser, main


def test_cleanup_does_not_require_consistent_spark_settings(
    monkeypatch,
    capsys,
):
    calls = []

    def cleanup(**kwargs):
        calls.append(kwargs)
        return [{"id": "workspace-id"}]

    monkeypatch.setenv("FABRIC_SPARK_RUNTIME_LANE", "runtime2")
    monkeypatch.setenv("FABRIC_SPARK_RUNTIME_VERSION", "1.3")
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.cleanup_ci_workspaces",
        cleanup,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "fabric-sandbox",
            "cleanup",
            "--repository",
            "owner/repository",
        ],
    )

    assert main() == 0
    assert calls[0]["repository"] == "owner/repository"
    assert capsys.readouterr().out == "found 1 CI sandbox workspace(s)\n"


@pytest.mark.parametrize("confirm", [False, True])
def test_cleanup_forwards_dry_run_and_confirmation(monkeypatch, capsys, confirm):
    calls = []

    def cleanup(**kwargs):
        calls.append(kwargs)
        return [{"id": "one"}, {"id": "two"}]

    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.cleanup_ci_workspaces",
        cleanup,
    )
    argv = [
        "cleanup",
        "--repository",
        "owner/repository",
        "--minimum-age-hours",
        "2.5",
    ]
    if confirm:
        argv.append("--confirm")

    assert main(argv) == 0
    assert calls == [
        {
            "confirm": confirm,
            "repository": "owner/repository",
            "minimum_age": timedelta(hours=2.5),
        }
    ]
    verb = "deleted" if confirm else "found"
    assert capsys.readouterr().out == (
        f"{verb} 2 CI sandbox workspace(s)\n"
    )


@pytest.mark.parametrize("confirm", [False, True])
def test_persistent_cleanup_forwards_ownership_boundary(
    monkeypatch,
    capsys,
    confirm,
):
    calls = []

    def cleanup(**kwargs):
        calls.append(kwargs)
        return [{"id": "persistent"}]

    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.remove_persistent_workspace",
        cleanup,
    )
    argv = [
        "remove-persistent",
        "--workspace-name",
        "fabricqueryr-dev-owner",
        "--owner-id",
        "owner-id",
        "--managed-by",
        "workflow.yaml",
        "--repository",
        "owner/repository",
    ]
    if confirm:
        argv.append("--confirm")

    assert main(argv) == 0
    assert calls == [
        {
            "workspace_name": "fabricqueryr-dev-owner",
            "owner_id": "owner-id",
            "managed_by": "workflow.yaml",
            "confirm": confirm,
            "repository": "owner/repository",
        }
    ]
    verb = "deleted" if confirm else "found"
    assert capsys.readouterr().out == (
        f"{verb} 1 persistent sandbox workspace(s)\n"
    )


@pytest.mark.parametrize("scope", ["all", "onelake"])
def test_discover_dispatches_only_the_requested_scope(
    monkeypatch,
    capsys,
    scope,
):
    settings = SimpleNamespace(manifest_path=Path("manifest.json"))
    manifest = SimpleNamespace(items=[{"id": "one"}, {"id": "two"}])
    calls = []
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.SandboxSettings.from_environment",
        lambda: settings,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.discover",
        lambda supplied: calls.append(("all", supplied)) or manifest,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.discover_onelake",
        lambda supplied: calls.append(("onelake", supplied)) or manifest,
    )

    assert main(["discover", "--scope", scope]) == 0
    assert calls == [(scope, settings)]
    assert capsys.readouterr().out == (
        "wrote manifest for 2 items: manifest.json\n"
    )


@pytest.mark.parametrize(
    ("missing", "expected_status", "expected_output"),
    [
        (
            [],
            0,
            "workspace definitions: workspace\n"
            "fixtures: fixtures\n"
            "manifest: manifest.json\n",
        ),
        (["missing.json"], 1, "missing: missing.json\n"),
    ],
)
def test_doctor_reports_configuration_state(
    monkeypatch,
    capsys,
    missing,
    expected_status,
    expected_output,
):
    settings = SimpleNamespace(
        validate_local_paths=lambda: missing,
        workspace_definition_dir=Path("workspace"),
        fixture_dir=Path("fixtures"),
        manifest_path=Path("manifest.json"),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.SandboxSettings.from_environment",
        lambda: settings,
    )

    assert main(["doctor"]) == expected_status
    assert capsys.readouterr().out == expected_output


@pytest.mark.parametrize("command", ["deploy", "seed"])
def test_mutating_commands_dispatch_once(monkeypatch, command):
    settings = object()
    calls = []
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.SandboxSettings.from_environment",
        lambda: settings,
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.deploy",
        lambda supplied: calls.append(("deploy", supplied)),
    )
    monkeypatch.setattr(
        "fabricqueryr_sandbox.cli.seed",
        lambda supplied: calls.append(("seed", supplied)),
    )

    assert main([command]) == 0
    assert calls == [(command, settings)]


@pytest.mark.parametrize("argv", [[], ["discover", "--scope", "invalid"]])
def test_parser_rejects_missing_commands_and_invalid_scopes(argv):
    with pytest.raises(SystemExit):
        build_parser().parse_args(argv)
