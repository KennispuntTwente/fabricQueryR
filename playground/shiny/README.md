# Persistent Fabric sandbox for Shiny

The integrated app and all Shiny/OAuth integration now live in
[shinyOAuthDB's Fabric example](https://github.com/lukakoning/shinyOAuthDB/tree/main/playground/fabric).
This repository manages the persistent data and capacity used by that app.

The app covers Warehouse and Lakehouse SQL, DAX JSON and Arrow, KQL, OneLake
CSV and Delta, mirrored Delta, paginated GraphQL, and workspace discovery.
It defaults to `fabricqueryr-shiny-dhrkoning`; set `FABRIC_SHINY_WORKSPACE`
to `fabricqueryr-dev-dhrkoning` to use the development workspace instead.

## Setup

Use the existing `FABRICQUERYR_TENANT_ID`, `FABRICQUERYR_CLIENT_ID`, and
`FABRICQUERYR_CLIENT_SECRET` settings for the shared sandbox application.
The app labels this shared identity explicitly. For each visitor's permissions,
configure delegated sign-in following the app's README. All native drivers and
fabricQueryR dependencies remain the same as for ordinary R queries.

## Start the dedicated Shiny sandbox

The [Manage persistent Fabric sandbox workflow](https://github.com/KennispuntTwente/fabricQueryR/actions/workflows/fabric-sandbox.yaml)
uses the same actions for both sandboxes. Select branch `master`,
`sandbox=shiny`, and `action=start`, or use these commands from the repository:

```sh
# Read the capacity state without changing anything.
gh workflow run fabric-sandbox.yaml --ref master -f sandbox=shiny -f action=status

# Resume F2 for up to one hour and prepare the app's persistent data.
gh workflow run fabric-sandbox.yaml --ref master -f sandbox=shiny -f action=start
```

`start` uses the existing `rpackagecap` F2 and creates a separate workspace,
`fabricqueryr-shiny-dhrkoning`. It seeds the Warehouse, Lakehouse, both DAX models,
Eventhouse/KQL, OneLake files and Delta tables, mirrored table, and GraphQL source
used by the app. The optional SQL Database and Warehouse snapshot are not
included. Choose `sandbox=development` for the full development fixtures in
`fabricqueryr-dev-dhrkoning`, using exactly the same start/status/pause actions
and one-hour shutdown. Both share F2; an active session must finish or be paused
before starting the other sandbox or the integration suite.

Wait for the workflow to finish successfully, then launch from R:

```r
# From the fabricQueryR repository, with shinyOAuthDB checked out alongside it:
pkgload::load_all(".")
pkgload::load_all("../shinyOAuthDB")
shiny::runApp("../shinyOAuthDB/playground/fabric", port = 8100)
```

Later starts reuse the workspace and its sample data. To reset the sample data,
select `reseed` in the workflow form or add `-f reseed=true` to `start`.
An interrupted first setup can be retried with another `start`.

### Automatic pause after one hour

Every `start` arms a separate shutdown workflow before it can resume F2.
The deadline is one hour from arming, including setup time, and appears in the
start run's summary with a link to its shutdown run. If shutdown cannot be armed,
the start fails without resuming F2. This also works from a feature branch and
does not depend on a scheduled workflow on the default branch.

The shutdown run continues after the setup run finishes or is cancelled. At its
deadline, it signs in again and pauses F2, retaining the workspace and its data.
Running `start` again while F2 is active keeps the original deadline. Once
F2 has paused, another start opens a new one-hour session. An old shutdown run
checks the session identifier before acting, so it does not pause a newer session.

The first setup may use a substantial part of the hour. The deadline still
applies during provisioning; if it interrupts setup, the next start retries the
incomplete fixtures. To finish earlier, pause manually:

```sh
gh workflow run fabric-sandbox.yaml --ref master -f sandbox=shiny -f action=pause
```

Starting F2 resumes capacity billing. Pausing affects every workspace assigned
to that capacity, including any outside this playground. See Microsoft's
[pause and resume documentation](https://learn.microsoft.com/en-us/fabric/enterprise/pause-resume)
for billing behavior. Failed/cancelled setup still attempts an immediate pause
of its session, including an already active session reused by the start. Cancelling its shutdown run also attempts an immediate
pause. Transient Azure HTTP errors are retried. GitHub runner loss, forced
cancellation or Azure outages can prevent shutdown, so this is an automatic
cost guard rather than a guaranteed billing cap. In those cases, check
`status` and use `pause` or the Azure portal if necessary.

`check-shutdown` exercises the same independent timer with a two-minute
deadline on an already paused F2. It never requests resume and refuses to run
on an active capacity. The internal timer now has its own **Fabric capacity
shutdown (internal)** workflow, dispatched automatically. The sandbox form only
asks for the sandbox, action, and optional reseed.

### Periodic backup check

The separate [Pause expired Fabric F2 workflow](https://github.com/KennispuntTwente/fabricQueryR/actions/workflows/shiny-capacity-watchdog.yaml)
checks the existing capacity every six hours, at 00:03, 06:03, 12:03 and 18:03 UTC.
It is installed on `master`,
where GitHub runs scheduled workflows, and can also be run manually. Its queue
is independent of the setup and one-hour shutdown jobs.

If F2 is active after this repository's recorded session deadline, the check pauses
it. It leaves paused capacities and sessions within their deadline alone. Invalid
shutdown metadata on a capacity tagged for this repository also triggers a pause;
an active capacity without that ownership tag reports an error for manual review.
The check rereads the session before pausing, retries transient request failures,
and never resumes capacity or changes workspace data.

This supplements the one-hour shutdown job. GitHub can delay scheduled runs and
automatically disables schedules in public repositories after 60 days without
repository activity, so the interval is not a strict maximum delay. See
[GitHub's schedule behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#schedule).

### GitHub configuration

These actions use the existing `fabric-integration` environment and Azure OIDC
login. `FABRIC_CAPACITY_ID__PAID` identifies the Fabric F2 capacity;
`AZURE_SUBSCRIPTION_ID`, `AZURE_TENANT_ID` and `AZURE_CLIENT_ID` configure Azure
access. The Azure resource defaults to resource group `fabric-rg` and capacity
`rpackagecap`; override them with `FABRIC_F2_RESOURCE_GROUP` and `FABRIC_F2_NAME`
if needed. The workflow checks that the Azure resource and Fabric ID refer to
the same F2 before starting it.

The CI identity needs Azure permission to read, resume and suspend this capacity,
and merge its shutdown tags (`Microsoft.Resources/tags/write`).
The startup job uses GitHub `actions: write` to dispatch the independent shutdown;
no personal access token is required. It waits until that run has verified the
lease and started its timer before resuming F2. The identity also needs
the existing Fabric permissions to assign a workspace and create/seed its
items. It grants the configured playground owner workspace Admin access. The
app still uses the connection settings described under Setup; starting the
workflow does not configure a delegated Web registration or host the Shiny app.

For one-time Azure administrator setup, the repository includes a narrow
[capacity role](../../infra/fabric/shiny-capacity-role.json) and a setup script:

```powershell
# Preview the role and target first, then apply it to the CI identity.
./tools/fabric-sandbox/configure-shiny-capacity-role.ps1
./tools/fabric-sandbox/configure-shiny-capacity-role.ps1 -Apply
```

The script reads the CI identity from the GitHub environment and assigns the
role on the named F2 only. It permits reading, resuming, suspending and tagging
that resource; it does not grant capacity creation, resizing, deletion or access
management. Running the script does not start F2.
