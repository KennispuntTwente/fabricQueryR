# Shiny integration acceptance

Implementation: 29 September 2026. Scope: explicit delegated Fabric discovery,
SQL and GraphQL; one fixed tenant; session retention; synchronous calls.
Updated 30 September 2026 with asynchronous token acquisition and query workers
using Shiny `ExtendedTask` and `promises::future_promise()`.
The same day's service expansion adds DAX, KQL and OneLake, with a complete
example app covering each data service.

The user requested local implementation/testing and explicitly deferred browser
acceptance. Do not describe an offline test or a service-principal sandbox pass
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
