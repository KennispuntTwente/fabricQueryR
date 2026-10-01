# Shiny integration acceptance

Implementation: 29 September 2026. Scope: explicit delegated Fabric discovery,
SQL and GraphQL; one fixed tenant; session retention; synchronous calls.
Updated 30 September 2026 with asynchronous token acquisition and query workers
using Shiny `ExtendedTask` and `promises::future_promise()`.
The same day's service expansion adds DAX, KQL and OneLake, with a complete
example app covering each data service.

The user requested local implementation/testing and explicitly deferred delegated
browser acceptance. Do not describe an offline test or a service-principal sandbox pass
as delegated-user evidence. No registration, consent or test-user grants were
changed during this implementation.

## Local execution

Install the sibling shinyOAuth checkout first. Its target-enabled development
version is `0.6.1.9000`; fabricQueryR checks the required public capabilities
rather than assuming that every installation with a development version has
them. It does not use private shinyOAuth state in production code. Tests inject
synthetic accepted tokens at the upstream module's acceptance seam.

```powershell
rtk proxy R CMD INSTALL ../shinyoauth
rtk proxy Rscript -e "devtools::test(filter = '^(fabric_shiny|shiny-example)', stop_on_failure = TRUE, reporter = 'summary')"
rtk proxy Rscript -e "devtools::test(stop_on_failure = TRUE, reporter = 'summary')"
rtk proxy Rscript -e "devtools::document(); pkgdown::check_pkgdown()"
```

Covered locally:

- Target declarations and SQL's double-slash scope; undeclared resources/scopes.
- Configured host checks, HTTPS, redirects, SQL retry policy propagation and
  rejection of unconfigured Delta access before acquisition.
- Bounded 401 refresh and a 403 without refresh/retry.
- Real shinyOAuth module references across rotation, logout, replacement,
  foreign sessions and session closure; ordinary R6 workspace credentials.
- Two simultaneously active mock Shiny sessions with distinct identities;
  each can use its own provider and rejects the other's provider.
- Real Microsoft target acquisition/rotation logic using synthetic HTTP
  responses, with SQL/GraphQL tokens delivered to their respective transports.
- Optional-target failure, explicit retry, redacted state, initial UI/server.
- Runnable example result clearing and absence of automatic read replay after
  a new login.

The full offline suite skips external Fabric, Python oracle and runtime lanes
when not opted in. Those skips are not evidence for the scenarios below.

Initial validation results (29 September):

- Full offline suite: **7,840 passing expectations, 0 failures, 0 errors,
  0 warnings, 181 skipped test cases** in optional/external lanes.
- Focused Shiny/example tests: passed without skips.
- Vignette signature and semantic execution tests: passed, including the new
  fixed-endpoint SQL app. The vignette also rendered to HTML successfully.
- `pkgdown::check_pkgdown()`: no problems found.
- `R CMD check --no-manual --ignore-vignettes`: **0 errors, 0 warnings, 0 notes**.
  Used the package files in a clean temporary staging directory to avoid
  copying local development/check artifacts, and set `LC_ALL=C`, `LANG=C` and
  `LANGUAGE=en` for child processes because this Windows R setup rejects
  `C.UTF-8`. The new vignette was rendered separately.

## ExtendedTask update (30 September)

`fabric$access_token(service, async = TRUE)` resolves a fixed token for one
service while retaining endpoint policy through serialization. Acquisition
rejects an authorization replaced or closed before the token is delivered.
The vignette and packaged app contain the same complete `ExtendedTask` example.
Only resolved access tokens and ordinary query inputs enter the query worker;
the owning Shiny process retains OAuth connections and refresh credentials.

Focused validation:

- Shiny facade, token-provider and asynchronous-acquisition tests passed.
- A real background R process ran the example's task with a synthetic SQL
  transport. A second Shiny input and logout were handled while the worker was
  held open. Its result was discarded after login changed; an explicit new
  invocation displayed the new user's result. The worker PID differed from
  the Shiny process PID.
- Vignette execution and signature checks passed. The rendered HTML contains
  the complete app, and `pkgdown::check_pkgdown()` found no problems.
- `devtools::test(stop_on_failure = TRUE, reporter = "summary")` passed with
  no failures or test warnings; 181 optional/live test cases were skipped.
- A clean staged package passed
  `R CMD check --no-manual --ignore-vignettes --no-tests` with 0 errors,
  0 warnings and 0 notes. The full test suite and vignette rendering were run
  separately, as above.

The worker test uses a synthetic query implementation; it does not establish
delegated Fabric access. Browser acceptance remains pending below.

## Additional data services (30 September)

Added DAX, KQL and OneLake profiles. DAX and GraphQL combine their declared
scopes in one Power BI target while retaining separate endpoint policies.
Exact audience routing takes priority over overlapping scope overrides.

`devtools::test(filter = "^fabric_shiny", stop_on_failure = TRUE,
reporter = "summary")` passed without test warnings or skips. The new service
test executes real shinyOAuth target acquisition with synthetic HTTP responses,
then sends the resulting credentials through the package's DAX, GraphQL, KQL
and OneLake transports. It checks that DAX and GraphQL reuse one acquired token,
and exercises asynchronous token snapshots for DAX and KQL.

This is local execution evidence only. Delegated service permissions and
browser acceptance remain pending.

## Example data-source coverage (30 September)

The vignette and packaged app now contain the same complete app with seven
choices: semantic-model DAX, Warehouse SQL, Lakehouse SQL, Eventhouse KQL,
OneLake files, OneLake Delta tables and GraphQL. DAX is the initial choice and
uses a year filter, an existing model measure and a Shiny chart. The selected
source determines the service token passed into the `ExtendedTask` worker.

Focused `shiny-example` and `vignettes` tests pass. They exercise all seven
branches, the DAX chart, service selection, query inputs, and stale-result
checks after changing year, source or sign-in. The real-worker responsiveness
test now exercises the DAX branch with a synthetic query transport. It still
proves that Shiny handles another input and logout while the worker is held,
and that a result from a previous sign-in is suppressed.

The full offline suite (`devtools::test(stop_on_failure = TRUE,
reporter = "summary")`) passes without test failures or warnings, with 181
optional/live cases skipped. The vignette renders successfully and
`pkgdown::check_pkgdown()` passes.
A clean staged `R CMD check --no-manual --ignore-vignettes --no-tests` reports
0 errors, 0 warnings and 0 notes. The delegated/browser gate below remains
pending, as requested; none of these local checks substitutes for it.

## Mirai preference (30 September)

The complete example selects mirai when installed and falls back to
`promises::future_promise()` otherwise. Both use `ExtendedTask`. The mirai
expression receives the reader function, resolved service token and query
inputs explicitly, and loads fabricQueryR in its worker. An app-level
`onStop()` callback shuts down the selected worker pool.

`devtools::test(filter = "^(shiny-example|vignettes)$",
stop_on_failure = TRUE, reporter = "summary")` passes. The responsiveness test
runs once with real mirai daemons and once with a real future worker. Each
backend handles another input and logout during a held query, suppresses stale
results after a new sign-in, propagates query errors, and shuts down through
the registered callback. No live Fabric calls were needed for this backend
change; the delegated/browser gate remains pending.

The full offline suite also passes with no test failures or warnings and 181
optional/live cases skipped. Vignette rendering and `pkgdown::check_pkgdown()`
pass. A clean staged `R CMD check --no-manual --ignore-vignettes --no-tests`
reports 0 errors, 0 warnings and 0 notes; tests and rendering ran separately.

## SQL introduction (30 September)

The vignette now starts with Warehouse/Lakehouse SQL setup and queries, then
introduces the DAX dashboard. The full app lists SQL first and initially
selects Warehouse; its mirai preference and query behavior are unchanged.

`devtools::test(filter = "^(fabric_shiny_config|shiny-example|vignettes)$",
stop_on_failure = TRUE, reporter = "summary")` passes, including both real
worker backends. Vignette rendering and `pkgdown::check_pkgdown()` also pass.

## Persistent sandbox playground (30 September)

`playground/shiny` now launches with the existing sandbox credentials or a
separately configured delegated Web registration. It discovers fixture addresses,
uses the current checkout in its workers, and provides SQL (ODBC/ADBC), JSON and
Arrow DAX, KQL, CSV, Delta, mirrored Delta, paginated GraphQL and item discovery.
The existing fixture definitions already supply these sources; no new fixture
definitions are needed. Missing targets are labelled and omitted from the batch
query so the remaining sources can still be used.

Validation during this change:

- `devtools::test(filter = "^playground", stop_on_failure = TRUE)` passes,
  including real mirai worker responsiveness, logout/account replacement,
  stale-filter suppression, SQL parameters and GraphQL pagination envelopes.
- The full offline suite passes without test failures or warnings; 181
  optional/live tests are skipped. The final additional GraphQL regression
  test also passes in the focused playground run.
- A headless Chrome run with `shinytest2::AppDriver` launched the actual app
  against the persistent sandbox. Shared-identity JSON DAX returned categories
  A = 10.5 and B = 20, drew the chart, and returned only B = 20 after filtering.
  The responsiveness counter worked, and the batch results retained successful
  queries alongside a failed Arrow query.
- The actual future dispatcher also ran the live category-B DAX query through
  `ExtendedTask` in a separate process. It returned B = 20. The installed future
  package emits its existing R patch-version build warning in that process.
- Live item discovery currently returns only the two semantic models. Arrow
  DAX returns HTTP 401 with `PowerBINotLicensedException`. SQL, KQL, OneLake,
  mirrored tables and GraphQL could not be tested live because their fixtures
  are absent from the workspace's API inventory.

The CI-configured capacity is absent from the Fabric and Power BI capacity lists
using both the sandbox application and the cached Azure CLI user. The user-level
Power BI admin capacity list also contains only the suspended paid F2 and the
reserved PPU capacity. The user explicitly prohibited starting the paid capacity;
no capacity, workspace assignment or fixture was changed. Restoration and the
remaining live checks are pending identification/access to an active trial
capacity. This does not establish why the previous trial is absent.

Launch and delegated-mode instructions are in
[`playground/shiny/README.md`](../../playground/shiny/README.md). Shared-identity
browser checks above do not satisfy the delegated two-user gate below.

## Dedicated F2 workflow (1 October)

The manual persistent-sandbox workflow now also accepts `shiny-status`,
`shiny-start` and `shiny-pause`. The start action targets the existing paid F2
and prepares only the main app fixtures in `fabricqueryr-shiny-dhrkoning`.
It reuses completed fixtures on later starts; an explicit `reseed` resets their
sample data. Pause retains the workspace. General sandbox rebuild and teardown
remain separate actions and do not run for these Shiny operations.

The controller verifies the Azure resource's F2 SKU and matching Fabric identity
before resuming. Failed/cancelled starts attempt to pause only a capacity resumed
by that run. A successful start leaves F2 active until `shiny-pause` is requested.
The app selects this workspace through `FABRIC_SHINY_WORKSPACE`; its existing
default and authentication modes are unchanged.

Local validation passes:

- All 255 sandbox Python tests, including capacity transitions, repeated starts,
  interrupted setup, ownership checks and failure handling.
- The Lakehouse fixture test writes and reads real typed Parquet bytes, exercises
  the load operation's HTTP polling and verifies SQL metadata refresh/readiness.
- Focused R playground tests, R formatting, Python lint/format checks and
  `actionlint` for the modified workflow.
- A live read-only controller call confirms the configured Azure F2 and Fabric
  capacity match and reports `Paused`.
- GitHub Actions [run 36917306894](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36917306894)
  successfully dispatched `shiny-status` from `shiny-integration`, authenticated
  through OIDC and verified the F2 resource and Fabric ID. All paid mutation
  steps and the general sandbox job were skipped.

Paid activation and live provisioning of the new workspace have not been run.
The user requested the action; its paid start is left for an explicit dispatch.
The prior browser evidence applies to the general sandbox's available JSON DAX
model, not these newly defined fixtures. The delegated browser gate remains
pending as previously agreed.

## One-hour automatic shutdown (1 October)

Shiny startup now records a one-hour shutdown lease on the existing Azure F2,
dispatches an independent `shiny-watchdog` workflow, and waits for that run to
verify the lease and enter its timer before resuming. A missing, failed or
different-revision watchdog blocks resume. Repeated starts preserve an active
session's deadline. Shutdown checks the lease identifier so an old run leaves a
new session alone. Setup time counts toward the hour, including first provisioning.

The independent run uses fresh Azure OIDC authentication after waiting and also
attempts shutdown when its timer is cancelled or fails. Setup failure retains its
immediate, lease-scoped cleanup. Shutdown retries transient HTTP errors after
re-reading capacity state. Runner loss, forced cancellation and Azure/GitHub
outages remain outside this guarantee; this is not an absolute billing cap.
The previous section's manual-only shutdown behavior is superseded.

Local validation:

- All 274 sandbox Python tests pass, including dispatch/readiness, rejection
  before resume, expiry, repeated starts, stale session isolation, cancellation
  during resume, lost suspend responses and a no-resume shutdown check.
- The timer also ran against the real local clock with a short deadline.
- Python lint/format checks and workflow `actionlint` pass.

The first live no-resume check found that the CI identity had only Azure Reader
access: writing the expiry tags returned HTTP 403 before dispatch or resume.
The checked-in `fabricqueryr-shiny-f2-controller` role now grants only capacity
read/resume/suspend and resource tag read/write, assigned on `rpackagecap` itself.
The administrator setup script was previewed, applied and previewed again; the
assignment scope and five actions were verified. No capacity start was requested.

After configuring that role, the live no-resume check
[36919347243](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36919347243)
passed. It wrote the lease through Azure's tag API and used the workflow's
`GITHUB_TOKEN` to dispatch independent shutdown run
[36919439399](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36919439399)
from the feature branch. That run verified the lease, waited until the recorded
two-minute test deadline, signed in again through OIDC and successfully ran the
pause path against the already paused F2. It did not create a workspace, seed
fixtures or request resume. Active-to-paused execution after a paid start remains
untested; its state transitions and error handling are covered locally.

A second check
[36919735175](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36919735175)
armed another shutdown run. Cancelling that run
([36919795835](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36919795835))
while its timer was running cancelled the wait, then both fresh OIDC login and
the pause step completed successfully before the deadline. Its overall
`cancelled` conclusion is expected. F2 remained paused throughout both checks.

## Periodic capacity backup (1 October)

`shiny-capacity-watchdog.yaml` adds a pause-only check at minutes
3, 8, 13, ..., 58 of each hour. It uses a separate concurrency group and the
existing Azure OIDC identity. The workflow, standard-library controller and its
unit tests are also installed on `master` so scheduling is active without merging
the Shiny feature. No Fabric provisioning paths or permissions were changed.

The controller only reads the exact configured Azure F2 and invokes suspend.
It pauses expired sessions owned by this repository, also failing closed on
invalid owned lease metadata. It leaves paused/unexpired sessions alone and
reports an error for an active capacity without the repository's ownership tag.
Before suspension and on retries, it rereads state and the session fingerprint.
The independent one-hour shutdown remains the primary timer; GitHub's scheduled
jobs can be delayed or disabled after public-repository inactivity.

Validation passes locally:

- All 285 Python tests across the sandbox and standalone watchdog, including
  startup-lease compatibility, deadline boundaries, session replacement,
  invalid metadata, transition handling and an accepted suspend with a lost
  response. The ten standalone tests also pass in the isolated `master` checkout.
- Python lint/format checks and `actionlint` for the new workflow.
- The actual standalone controller executed through the cached Azure CLI login
  and reported `F2 already paused`; no capacity state change was requested.

The three standalone files were enabled on `master` in commit `2538827f`.
GitHub lists the scheduled workflow as active. A manual execution of that exact
workflow, [run 36921804752](https://github.com/KennispuntTwente/fabricQueryR/actions/runs/36921804752),
passed its ten tests, Azure OIDC login and live capacity check, reporting
`F2 already paused`. No paid capacity was started. The first automatically
scheduled invocation was not needed for this validation and has not been observed
at the time of this record; GitHub's five-minute schedule is enabled.

## Pending delegated/browser gate

Use a dedicated Web registration and two restricted users in the target tenant.
Use app-owned fixtures; do not modify production data merely to run acceptance.
Record only nonsecret test outcomes, timestamps and scope/expiry metadata.

| Scenario | Required evidence | Status |
| --- | --- | --- |
| Sign-in | Fixed tenant, PKCE/nonce/ID-token validation, no Graph UserInfo request; Fabric and SQL acquired from one login | Pending |
| Simultaneous users | Each browser session queries its own permitted rows; an item-only user does not need workspace listing | Pending |
| Refresh | Expiry and one forced 401 acquisition preserve local generation; refreshed credentials reach the correct service | Pending |
| Consent/tenant policies | Rejected consent and interaction-required recovery leave useful UI; no login loop | Pending |
| Logout/replacement | Clear rows and references in each tab, reject old items/providers; failed replacement cannot revive old access | Pending |
| SQL writes | Parameterized insert succeeds for writer and fails for reader; close connections and never replay ambiguous writes | Pending |
| GraphQL | Driver-free query; configured source SSO enforces the intended user's source permissions | Pending |
| DAX | Query a semantic model with each user's Read/Build access and applicable model security; reuse a model measure in Shiny | Pending |
| KQL/OneLake | User-specific Eventhouse queries, file reads and Delta reads with the appropriate service token | Pending |
| Hosting | Exact HTTPS redirect, callback subpath, proxy headers/cookies and WebSocket routing in the chosen deployment | Pending |
| Network policy | Standard/private Fabric endpoint variants work; unconfigured gateways/redirects fail without credential delivery | Pending |

## Follow-up scope

The broader design's service-breadth and performance phases remain open:
Livy, jobs/functions, bulk-write workflows, static
`.default` consent, retained/multiple-worker authorizations and background-task
credential renewal. Do not advertise them as supported by this first facade.
General fabricQueryR functions still support their existing authentication modes.
