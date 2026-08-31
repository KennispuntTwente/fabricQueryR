import sys

from fabricqueryr_sandbox.cli import main


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
