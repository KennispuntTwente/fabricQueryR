# Review fixes and execution evidence — 2026-09-16

This records the earlier review baseline. The later
[follow-up validation](review-followup-validation.md) records additional fixes,
including the resolution of the Livy discovery failure described here.

The changes following review baseline `684a86a` address the implementation defects
and add explicit coverage for the missing scenarios. Some service evidence remains
blocked or unconfigured; adding a test does not establish that its Fabric behavior
works. Each implementation fix or distinct coverage gap has its own commit.

## Issue disposition

| Review issue | Change | Execution evidence and limits |
| --- | --- | --- |
| JSON DAX Variant type loss | Preserve JSON number provenance and distinguish numeric-looking text, including oversized integers. | Unit cases and live JSON/Arrow comparisons passed, including row order and nulls. |
| Warehouse narrowing | Compare destination decimal/temporal metadata before destructive SQL; retain the input timestamp precision before Parquet normalization. | Unit tests and 14 live assertions passed: append/overwrite rejection, unchanged existing rows, and compatible millisecond overwrite. Other SQL coercions are outside this safeguard. |
| Warehouse COPY permissions | Require the documented Contributor access on source and destination workspaces; distinguish Workspace Identity. | Documentation corrected against Microsoft's COPY contract; granular permissions are no longer claimed sufficient. |
| Livy GUID casing | Validate then compare normalized GUIDs for session, HC and batch attachment. | Unit cases and live uppercase attachment passed. |
| Introspection success hidden by disabled setting | Separate disabled and enabled tests; require schema, nested types and collection results in the enabled case. | Default-audience R6 GraphQL reads and disabled HC0046 response passed. Enabled fixture is still absent. |
| HC lifecycle and delegated discovery | Run lifecycle assertions independently of packing; add required packed and delegated cases, with independent session/batch discovery tests. | Both general HC lifecycle and an explicit `require_packed = TRUE` run passed 19 assertions. Delegated discovery evidence is recorded below. |
| Restricted identities and RLS | Add two-identity access/denial matrix, opposing OneLake grants, and distinct JSON/Arrow RLS row sets for impersonation, roles and customData. | Fixture validation tests passed. Restricted identities and the dedicated RLS model are not configured. |
| Execution options | Execute Notebook Python/jar/archive dependencies and Spark Python libraries; add required custom-pool/mount, cross-workspace and DataWarehouse notebook probes. | Notebook dependencies, including executor archive access, and Spark library import passed live. Other compute fixtures are not configured. |
| Functions | Require published fixtures in the designated feature group; accept documented 409 or 500 for an unhandled exception while checking failure details. | Unit fixture checks passed. Published function URLs are absent. |
| Shortcut transforms/cache/providers | Required groups fail missing prerequisites; seven providers have separate cases. | Provider configuration tests passed. CSV-to-Delta was rejected by the development tenant; provider connections are absent. Delegated cache reset returned `ExternalShortcutCacheDisabled`; the application identity returned `PrincipalTypeNotSupported`. |
| KQL recovery | Exercise mixed valid/missing sources, per-source outcomes, and recovery after initial staging-cleanup failure; add a service-cancellation fixture. | Mixed-source and cleanup-recovery live cases passed. Only the first cleanup failure was injected; real storage denial and service cancellation are unverified. |
| Schedules | Require non-UTC Daily firing with a unique marker; add Spark schedule update coverage and configured workload-route cases. | Spark schedule CRUD passed. Non-UTC firing failed in two twenty-minute attempts despite accepted schedules. Dataflow, materialized Lake View and dbt fixtures are absent. |
| Pipeline history | Start and wait for the test's own run, then locate its ID in history. | Live test passed. |
| Livy contradictory wait states | Give explicit failure/cancellation/error precedence over idle readiness. | Synthetic contradictory responses passed; ordinary live wait passed. |
| Livy DATE/BYTE/SHORT robustness | Enforce date format/calendar validity and narrow integer ranges. | Invalid-value unit tests and valid live SQL results passed. |
| GraphQL null rows | Preserve nullable list elements as missing rows, retaining their positions in `null_rows`. | Unit pagination/null/partial-result tests passed. A Fabric nullable-element schema fixture is unavailable. |

## Validation commands

Delegated Livy session creation, readiness and a Spark action succeeded using a
refreshed cached user credential, but the session collection remained empty.
Discovery failed both the initial attempt and the retry after executing a Spark
job. The independent batch test also submitted successfully but received an empty
collection throughout polling. This evidence does not establish a client-side
cause, and discovery remains unresolved; the new required tests expose the failures.

- `devtools::test(stop_on_failure = TRUE)`: the full offline suite passed 7,124
  expectations with zero failures, errors or test warnings. Live cases were
  skipped. The final session/batch split was also loaded in a focused offline run.
- `devtools::check(document = FALSE, manual = FALSE)`: zero errors and warnings;
  one environment note, `unable to verify current time`.
- `pkgdown::check_pkgdown()`: passed.
- `uv --directory tools/fabric-sandbox run --locked pytest -q`: 211 passed,
  including workflow/test-file contracts.

Live checks used the checked-out package and the marked persistent development
workspace. The standard SQL integration runner stopped on an unrelated stale
seed revision. Self-contained probes therefore used
`connect_playground_sandbox()` to verify ownership and discover targets, then
executed the permanent test bodies with those fixture identifiers. Service calls
were real, apart from the explicitly injected initial KQL cleanup failure.
No full workspace reseed or reprovision was performed.

The tests remove their own temporary tables, files, schedules and compute handles.
After both schedule attempts, a fresh list confirmed that their schedules were
gone and no new scheduled notebook jobs had appeared.

See [feature-lanes.md](feature-lanes.md) for required-feature names, fixture
contracts and runner filters. Missing optional prerequisites remain explicit
skips in ordinary runs and failures in their designated required groups.
