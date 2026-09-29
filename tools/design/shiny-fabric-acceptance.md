# Initial Shiny integration acceptance

Implementation: 29 September 2026. Scope: explicit delegated Fabric discovery,
SQL and GraphQL; one fixed tenant; session retention; synchronous calls.

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

Validation results:

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
| Hosting | Exact HTTPS redirect, callback subpath, proxy headers/cookies and WebSocket routing in the chosen deployment | Pending |
| Network policy | Standard/private Fabric endpoint variants work; unconfigured gateways/redirects fail without credential delivery | Pending |

## Follow-up scope

The broader design's service-breadth and performance phases remain open:
OneLake/Storage, Kusto, DAX, Livy, jobs/functions, bulk-write workflows, static
`.default` consent, retained/multiple-worker authorizations and background-task
credential renewal. Do not advertise them as supported by this first facade.
General fabricQueryR functions still support their existing authentication modes.
