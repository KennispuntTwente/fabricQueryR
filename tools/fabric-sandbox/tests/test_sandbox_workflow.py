from pathlib import Path

import yaml

ROOT = Path(__file__).parents[3]


def workflow(name):
    return yaml.load(
        (ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader
    )


def test_both_sandboxes_share_manual_actions_and_default_to_read_only_status():
    config = workflow("fabric-sandbox.yaml")
    assert set(config["on"]) == {"workflow_dispatch"}
    inputs = config["on"]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {"sandbox", "action", "reseed"}
    assert inputs["sandbox"]["options"] == ["development", "shiny"]
    assert inputs["action"]["options"] == ["status", "start", "pause", "check-shutdown"]
    assert inputs["action"]["default"] == "status"
    assert inputs["reseed"]["default"] == "false"
    assert set(config["jobs"]) == {"manage"}
    job = config["jobs"]["manage"]
    steps = job["steps"]
    start = next(s for s in steps if s.get("id") == "capacity")
    assert (
        start["if"] == "inputs.action == 'start' || inputs.action == 'check-shutdown'"
    )
    assert (
        start["env"]["SHUTDOWN_ACTION"]
        == "${{ inputs.action == 'start' && 'start' || 'check' }}"
    )
    assert '--purpose "$SANDBOX"' in start["run"]
    assert "vars.FABRIC_CAPACITY_ID__PAID" in job["env"]["CAPACITY_ID"]
    scripts = "\n".join(s.get("run", "") for s in steps)
    assert "terraform" not in scripts and "remove-persistent" not in scripts
    assert "capacity resume" not in scripts
    driver = next(
        s for s in steps if s.get("name", "").startswith("Install SQL driver")
    )
    assert steps.index(driver) < steps.index(start)


def test_startup_is_bounded_and_failures_pause_even_a_reused_session():
    config = workflow("fabric-sandbox.yaml")
    steps = config["jobs"]["manage"]["steps"]
    verify = next(s for s in steps if s.get("id") == "session")
    prepare = next(s for s in steps if "persistent_sandbox prepare" in s.get("run", ""))
    assert "autopause verify-active" in verify["run"]
    assert steps.index(verify) < steps.index(prepare)
    assert prepare["if"] == verify["if"] == "inputs.action == 'start'"
    assert "DEADLINE - $(date +%s) - 180" in prepare["run"]
    assert "timeout --signal=TERM --kill-after=30s" in prepare["run"]
    cleanup = next(s for s in steps if s.get("id") == "cleanup_login")
    assert "always()" in cleanup["if"] and "failure() || cancelled()" in cleanup["if"]
    assert "lease_id != ''" in cleanup["if"] and "resumed" not in cleanup["if"]
    pause = steps[-1]
    assert "steps.cleanup_login.outcome == 'success'" in pause["if"]
    assert (
        "autopause pause" in pause["run"] and '--lease-id "$LEASE_ID"' in pause["run"]
    )


def test_shutdown_is_independent_of_startup_and_refreshes_login_after_wait():
    public = workflow("fabric-sandbox.yaml")
    guard = workflow("fabric-capacity-shutdown.yaml")
    assert set(guard["on"]) == {"workflow_dispatch"}
    assert set(guard["on"]["workflow_dispatch"]["inputs"]) == {"lease_id", "guard_key"}
    assert "github.run_id" in guard["concurrency"]["group"]
    assert "fabric-shutdown" in guard["concurrency"]["group"]
    assert "fabric-integration" in public["concurrency"]["group"]
    assert "inputs.action == 'pause'" in public["concurrency"]["group"]
    assert "fabric-pause" in public["concurrency"]["group"]
    steps = guard["jobs"]["shutdown"]["steps"]
    verify = next(s for s in steps if s.get("id") == "lease")
    wait = next(s for s in steps if s.get("name") == "Wait for automatic pause")
    login = next(s for s in steps if s.get("id") == "shutdown_login")
    assert steps.index(verify) < steps.index(wait) < steps.index(login)
    assert "always() && steps.lease.outcome == 'success'" == login["if"]
    assert '--lease-id "$LEASE_ID"' in steps[-1]["run"]
    assert "steps.shutdown_login.outcome == 'success'" in steps[-1]["if"]
    assert all("resume" not in s.get("run", "") for s in steps)
