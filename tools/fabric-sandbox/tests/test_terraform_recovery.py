import os
from pathlib import Path
import shutil
import subprocess
import textwrap

import pytest


WORKFLOWS = ["integration-fabric.yaml", "fabric-sandbox.yaml"]
WORKFLOW_DIR = Path(__file__).parents[3] / ".github/workflows"
MIRROR_TIMEOUT = (
    "Error: unknown error\n"
    "  with fabric_mirrored_database.test,\n"
    "context deadline exceeded\n"
)


def provisioning_script(workflow):
    step = workflow.read_text().split(
        "- name: Create workspace and test targets", maxsplit=1
    )[1].split("- name: Export Terraform outputs", maxsplit=1)[0]
    return textwrap.dedent(step.split("run: |\n", maxsplit=1)[1])


def run_step(
    tmp_path, script, *, error="", first_status=1, state_present=True,
    retry_status=0, scope="all",
):
    bash = shutil.which("bash")
    if os.name == "nt":
        git = shutil.which("git")
        if git:
            candidate = Path(git).parent.parent / "bin/bash.exe"
            if candidate.is_file():
                bash = str(candidate)
    if not bash:
        pytest.fail("Bash is required to execute Terraform workflow tests")

    (tmp_path / "error.log").write_text(error, encoding="utf-8")
    env = os.environ.copy()
    env.update(
        RUNNER_TEMP=".", TF_DIR="sandbox directory", GITHUB_ENV="github-env",
        TF_VAR_fixture_scope=scope, TF_VAR_provision_sql_database="true",
        FIRST_STATUS=str(first_status), RETRY_STATUS=str(retry_status),
        STATE_STATUS="0" if state_present else "1",
    )
    # An ordinary refresh reproduces the stuck provider read. Recovery must
    # actually request replacement without refresh, rather than just retry.
    fake = r'''
terraform() {
  printf '%s\t' "$@" >> calls.log
  printf '\n' >> calls.log
  shift
  case "$1" in
    state) return "$STATE_STATUS" ;;
    apply)
      local count
      count=$(cat count.txt 2>/dev/null || printf 0)
      count=$((count + 1))
      printf '%s' "$count" > count.txt
      if [ "$count" -eq 1 ]; then
        cat error.log
        return "$FIRST_STATUS"
      fi
      if [[ "$TF_VAR_provision_sql_database" == false ]]; then
        return "$RETRY_STATUS"
      fi
      if [[ " $* " == *" -refresh=false "* ]] &&
         [[ " $* " == *" -replace=fabric_mirrored_database.test "* ]]; then
        return "$RETRY_STATUS"
      fi
      printf 'context deadline exceeded during refresh\n'
      return 73 ;;
    destroy)
      if [[ " $* " == *" -refresh=false "* ]]; then
        return 0
      fi
      printf 'context deadline exceeded during refresh\n'
      return 73 ;;
    *) return 99 ;;
  esac
}
'''
    result = subprocess.run(
        [bash, "--noprofile", "--norc", "-eo", "pipefail"],
        input=fake + "\n" + script, text=True, capture_output=True,
        cwd=tmp_path, env=env, timeout=30,
    )
    calls = [
        line.rstrip("\t").split("\t")
        for line in (tmp_path / "calls.log").read_text().splitlines()
    ]
    return result, calls


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_mirrored_timeout_replaces_only_the_unseeded_fixture(tmp_path, workflow):
    result, calls = run_step(
        tmp_path, provisioning_script(WORKFLOW_DIR / workflow),
        error=MIRROR_TIMEOUT,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(calls) == 3
    assert calls[1][1:] == [
        "state", "show", "-no-color", "fabric_mirrored_database.test"
    ]
    assert calls[2] == calls[0] + [
        "-refresh=false", "-replace=fabric_mirrored_database.test"
    ]


@pytest.mark.parametrize("workflow", WORKFLOWS)
@pytest.mark.parametrize(
    ("error", "state_present", "call_count"),
    [
        (MIRROR_TIMEOUT, False, 2),
        (MIRROR_TIMEOUT.replace("mirrored_database", "warehouse"), True, 1),
        (MIRROR_TIMEOUT.replace("context deadline exceeded", "Forbidden"), True, 1),
    ],
)
def test_unrelated_errors_or_missing_state_do_not_replace_resources(
    tmp_path, workflow, error, state_present, call_count,
):
    result, calls = run_step(
        tmp_path, provisioning_script(WORKFLOW_DIR / workflow), error=error,
        first_status=7, state_present=state_present,
    )
    assert result.returncode == 7
    assert len(calls) == call_count
    assert all(not any(arg.startswith("-replace=") for arg in call) for call in calls)


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_replacement_failure_is_not_retried_or_hidden(tmp_path, workflow):
    result, calls = run_step(
        tmp_path, provisioning_script(WORKFLOW_DIR / workflow),
        error=MIRROR_TIMEOUT, retry_status=9,
    )
    assert result.returncode == 9
    assert len(calls) == 3


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_successful_provisioning_does_not_replace_anything(tmp_path, workflow):
    result, calls = run_step(
        tmp_path, provisioning_script(WORKFLOW_DIR / workflow), first_status=0,
    )
    assert result.returncode == 0
    assert len(calls) == 1


@pytest.mark.parametrize("workflow", WORKFLOWS)
def test_sql_capacity_fallback_still_uses_an_ordinary_apply(tmp_path, workflow):
    result, calls = run_step(
        tmp_path, provisioning_script(WORKFLOW_DIR / workflow),
        error="SqlDatabasePerCapacityLimitReached",
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls == [calls[0], calls[0]]
    assert (tmp_path / "github-env").read_text().strip() == (
        "TF_VAR_provision_sql_database=false"
    )


def test_teardown_deletes_saved_resources_without_a_readiness_refresh(tmp_path):
    workflow = (WORKFLOW_DIR / "integration-fabric.yaml").read_text()
    step = workflow.split("- name: Destroy Fabric sandbox", maxsplit=1)[1]
    script = step.split("run: ", maxsplit=1)[1].splitlines()[0]
    result, calls = run_step(tmp_path, script)
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(calls) == 1
    assert calls[0][1] == "destroy"
