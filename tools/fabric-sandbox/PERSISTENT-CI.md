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
- Use a run-specific integration lease. Refuse to borrow an active Shiny or
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
