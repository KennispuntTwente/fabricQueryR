# Persistent Fabric integration tests

## Findings

The previous integration workflow created a new core workspace and a new runtime
2.0 workspace for every run. It published every definition, seeded every data
service, replaced the Warehouse snapshot, and destroyed both workspaces at the
end. Terraform state existed only on the runner and in a one-day artifact. The
workflow did not resume or pause its assigned capacity.

In [the successful run on 23 September 2026](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/35899627218),
core provisioning took about 14 minutes, preview provisioning 10 minutes, and
the longest test group (item jobs) another 38 minutes. Total elapsed time was
about 52 minutes. Rebuilding was avoidable work, but it was not the only large
consumer. There is no billing export here establishing the cause of earlier
charges.

Fabric F capacity is billed while active. Pausing also settles accumulated
overage and smoothed compute; it does not erase work already performed. Retained
OneLake data and database backups have separate storage charges. An activation
deadline therefore bounds the intended active period, not the final bill.
See Microsoft's [pause/resume documentation](https://learn.microsoft.com/en-us/fabric/enterprise/pause-resume)
and [billing documentation](https://learn.microsoft.com/en-us/fabric/enterprise/azure-billing).

## Implementation contract

- Live tests require a manual dispatch by default. Ordinary pushes and pull
  requests retain offline validation without activating F2.
- Keep separate, repository-owned integration workspaces for core and preview.
  These are separate from the development and Shiny playground workspaces.
- Discover and reconcile existing items through Fabric APIs. Do not depend on
  an expiring Actions artifact to remember persistent infrastructure.
- Check source and runtime revisions stored with the data. Ordinary R changes
  do not trigger seeding. Publish changed item definitions independently of
  fixture data. An incomplete seed must be retried, never treated as ready.
- Tests overwrite their own scratch tables or remove their temporary resources.
  Reap interrupted test jobs, schedules, disposable notebooks and scratch files
  before reuse. Retain baseline fixture tables and Delta history.
- Prepare dependencies before activation wherever possible. Arm the independent
  shutdown run before resuming the existing F2. Never resize or create capacity.
- Use a run-specific integration lease. Refuse to borrow an active development, Shiny or
  another integration session. Do not extend the one-hour deadline on retry.
- Pause immediately after all test jobs settle, including failure. Cleanup
  failure must not prevent pause. The independent deadline job and the periodic
  default-branch watchdog remain separate fallbacks for cancellation and runner
  loss. GitHub scheduling and Azure requests can be delayed: these are layered
  safeguards, not a real-time shutdown guarantee.
- Preserve fixtures after success, failure and timeout. A first bootstrap or
  large change may exceed the time budget; fail and pause instead of silently
  extending it. Subsequent runs reuse completed preparation.

The old orphan-workspace janitor can still remove marked ephemeral CI leftovers;
it must never delete the persistent integration workspaces.

## Validation boundary

Local tests exercise reuse, interrupted preparation, ownership checks, lease
isolation and shutdown behavior. Paid activation and two consecutive full live
runs are separate acceptance checks; offline passes do not establish that every
Fabric service is ready or that the full suite fits an F2 one-hour session.

Verified on 1 October 2026:

- `uv --directory tools/fabric-sandbox run pytest -o addopts= -q`: 301 passed.
- `python -m unittest discover -s tools/ci/tests`: 10 passed.
- `actionlint` 1.7.12: integration, manual sandbox and periodic guard workflows
  passed (shellcheck and pyflakes integrations disabled).
- The capacity controller read the actual ARM resource and Fabric capacity ID;
  `rpackagecap` was F2 and Paused. Read-only workspace reconciliation found no
  existing core or preview integration workspace. No paid resume was requested.
- [Integration shutdown check](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36925668767)
  passed on Ubuntu: tooling tests, OIDC, lease tagging, independent guard dispatch
  and final pause verification. Provisioning and R live tests were deliberately
  skipped by `action=check-shutdown`.
- Its [independent two-minute timer](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36925751790)
  also passed, including fresh OIDC login at the deadline and lease-scoped pause
  verification. A final ARM read confirmed F2 remained Paused.
- The already enabled default-branch periodic guard also completed a real
  [scheduled run](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36925496422).
  It understands integration leases through the unchanged lease/repository/deadline
  tag keys; the separate purpose and run tags prevent sharing an active session.

Pending paid acceptance: first bootstrap, a second run reusing the same item IDs
without seeding, all selected R service tests, scratch recovery after an
interrupted live test, and an actual Active-to-Paused transition. The independent
timer targets one hour, while test processes reserve the final three minutes
for cleanup. No live evidence yet establishes the full suite's elapsed time on
this F2. Use a single lane first if bootstrap plus tests exceed the budget.

## Unified interactive sandboxes (2 October 2026)

The development and Shiny workspaces now use one manual sandbox selector and
the same status/start/pause/check-shutdown actions. Start reuses the workspace;
development fixture preparation shares the integration reconciliation engine
without the CI scratch reset. The old Terraform rebuild path is removed from
the workflow. Explicit reseed refreshes data, and deliberate workspace removal
remains available through the ownership-checked CLI.

Both interactive starts and integration runs dispatch
`fabric-capacity-shutdown.yaml`. Internal timer inputs no longer appear in the
sandbox form. Development, Shiny and integration leases cannot borrow another
active session. Manual sandbox pause is also scoped to the selected session.
The existing periodic guard understands all three through its unchanged tag keys.

Local validation covers first creation, consecutive reuse, selective refresh,
explicit reseed, old-trial capacity assignment without workspace replacement,
ownership rejection, optional SQL Database limits, read-only status, deadline
retention, and cross-session pause protection. The Python sandbox suite, separate
watchdog tests, workflow lint, and R playground/local-runner tests pass. Real
read-only calls for both sandbox profiles succeeded and verified F2 as Paused.

Paid provisioning, actual data queries in the unified sandboxes, and an
Active-to-Paused transition remain pending; no paid capacity was activated for
this change. Use `action=check-shutdown` to exercise OIDC and the independent
timer on an already paused F2.

GitHub validation on commit `7e44a74c`:

- [Push checks](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36935640926):
  323 sandbox tests and 10 watchdog tests passed; paid activation, provisioning,
  and live R tests were skipped.
- Read-only status passed for both
  [development](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36935722970)
  and [Shiny](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36935727404).
  The CI identity found the existing owned development workspace on its former
  capacity; the next start can assign it to F2 without recreating it.
- The [development check](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36935667399)
  and its [independent timer](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36935778248)
  passed, followed by the
  [Shiny check](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36936029721)
  and its [independent timer](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36936079795).
  Both timers verified the lease, waited for their deadline, signed in again,
  and confirmed pause. Neither requested resume. A final ARM read confirmed F2
  remained Paused.
