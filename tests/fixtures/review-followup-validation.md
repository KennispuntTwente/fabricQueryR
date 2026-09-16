# Review follow-up — 16 September 2026

Baseline: `8786fa35`. The six implementation/documentation findings have separate
fix commits. Three additional commits improve Delta recovery coverage, correct
the local runner's credential selection, and fix live Livy discovery. `NEWS.md`
and `README.md` are unchanged.

## Completed fixes

| Review finding | Commit | Change and evidence |
| --- | --- | --- |
| Warehouse dictionary columns bypass type safeguards | `517f8e01` | Validate the dictionary value type, retain its original precision for destination checks, and decode batches before Parquet normalization. Offline Arrow/Parquet tests pass. The permanent SQL narrowing/CTAS/COPY tests passed 41 live assertions, including unchanged destination rows after rejected append/overwrite, decimal values, nulls, UTC microseconds and unsigned bytes. |
| OneLake uppercase GUID listings fail | `58e82663` | Compare GUID item segments without case sensitivity while preserving named-item and child-path casing. Unit tests still reject other items, prefix lookalikes and paths outside the requested directory. The live uppercase/lowercase listing comparison passed both assertions. |
| Conflicting job/refresh identifiers select the wrong cancellation target | `deb6c152` | Reject simultaneous primary and alias selectors before lookup or transport, including raw GUIDs, handles and detail/instance records. Status/cancellation tests cover equal and conflicting identifiers. No real operation was canceled to test invalid arguments. |
| KQL Parquet datetime precision is unclear | `384352c0` | Explain that NULL uses milliseconds, show explicit microseconds and a seven-digit UTC text projection, and distinguish the decimal-only numeric policy. Fifteen live assertions passed for default/millisecond/microsecond Parquet, nulls and full-precision text. |
| OneLake page-size overflow | `0d43cebb` | Check the allowed range before integer coercion. Out-of-range values, including values above integer range and extreme doubles, now raise the intended validation error without a coercion warning. |
| Empty refresh-attempt end time implies completion | `d8d01bb6` | Infer completion from the parsed timestamp. Tests cover missing/empty/present end times, service errors and explicit status. This is a defensive parser case; no matching anomalous live response was claimed. |

The KQL behavior was checked against Microsoft's current
[export properties](https://learn.microsoft.com/en-us/kusto/management/data-export/export-data-to-storage#supported-properties).
The Warehouse live tests verify the actual SQL types and values rather than only
the request payload or staged schema.

## Additional fixes and coverage

- `fb294fd1`: the permanent live Delta authentication test sends an invalid bearer
  to OneLake and then uses a real refreshed token. It passed five live assertions,
  including the FALSE/TRUE refresh sequence and correct rows. An offline test
  writes a partial Arrow IPC file, injects a transport failure during spooling,
  and checks deletion before the full retry and after stream release. The help
  now documents the one-retry limit and fixed token per scan. The partial-spool
  failure remains deliberately injected; it is not presented as a live expiry.
- `69d3bb45`: cached sandbox credentials must match the requested delegated or
  application principal type. Previously a delegated run could select a cached
  application token before the identity check rejected it. Regression tests
  cover both cache orderings and missing matching credentials. The corrected
  selection was used for the delegated playground connection and live probes.
- `f9f81496`: Livy listings now recover the requested rows from count-only
  responses. Diagnostics with both Fabric and Power BI audiences found empty
  `items` with positive totals (54 sessions and six batches) when `$count=true`;
  the same calls with `$count=false` returned records. The fallback preserves the
  total and requested offset/page limit, with no extra request for truly empty or
  exhausted pages. After the fix, both permanent delegated discovery tests found
  their own session/batch and passed all three assertions in 30 seconds. A separate
  read returned 55 sessions and seven batches with either audience. Regression
  tests cover fallback, offset preservation, exhausted pages and unavailable
  totals. The help explains that totals and rows may come from separate requests.

## Validation

- Full offline R suite with `FABRIC_DELTA_RS_ORACLE_TESTS=true`: **7,353 passed
  assertions, zero failures/errors/test warnings, 163 skips**. This includes the
  locked local Delta runtime tests; the skips are primarily disabled live tests,
  unavailable feature fixtures and Windows-inapplicable cases.
- Sandbox Python suite: **211 passed** (`uv --directory tools/fabric-sandbox run
  --locked pytest -q`). The new integration filenames belong to the existing
  OneLake and KQL/GraphQL CI groups.
- `devtools::document()` and `pkgdown::check_pkgdown()`: passed.
- `devtools::check(document = FALSE, manual = FALSE)`, with child-process
  `LC_ALL=C`: **zero errors, zero warnings, one environmental note**:
  `unable to verify current time`. Tests and vignette rebuilds passed.
- `git diff --check`: passed. The final protected-file comparison confirms no
  changes to `NEWS.md` or `README.md`.

The standard SQL runner stopped before tests because the deployed full-fixture
revision differs from the checkout (`a036549e...` versus `d1455b84...`). No fixture
revision was forged and no full reseed/reprovision was performed. The focused
Warehouse, OneLake GUID, KQL datetime and Delta tests instead used
`connect_playground_sandbox()` to verify the workspace owner and discover live
targets, then executed their permanent test bodies. The write tests create their
own data. The Delta test independently checked that its table existed and
asserted the actual expected rows. This does not certify the full seeded fixture
revision.

The Warehouse temporary tables and KQL datetime export folders were checked
afterward: none remain. One temporary Warehouse table left by an interrupted
test process was removed by its exact name.
Both new notebook schedules and the temporary Spark schedules were removed;
the one pre-existing disabled notebook schedule remains unchanged.

## Integration findings still requiring service evidence or fixtures

These findings are not represented as fixed by passing offline tests.

| Surface | Current disposition |
| --- | --- |
| Non-UTC schedule firing | Both the required application lane and a fresh delegated-user comparison failed after twenty minutes with no matching scheduled run; each disabled Spark schedule CRUD case passed four assertions. Independent reads confirmed the Daily schedules were enabled, held the submitted UTC boundaries and Amsterdam clock times, and had produced no new scheduled jobs. The request follows the [documented UTC-boundary contract](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/create-item-schedule). No client-side cause was established, so this issue remains open. |
| Restricted identities and DAX security | The authorization matrix is absent. The two restricted identities, opposing storage grants and RLS/roles/customData model must be configured before the existing required authorization lane can supply evidence. |
| Enabled GraphQL introspection | The enabled endpoint is absent. Microsoft's [documented control](https://learn.microsoft.com/en-us/fabric/data-engineering/api-graphql-introspection-schema-export) requires a workspace administrator to enable introspection in API Settings. Disabled introspection is covered; it does not substitute for success. |
| User data functions | All three published function URL settings are absent. The checked-in scalar, structured and error fixtures still require publication by their delegated owner and Execute access for the test identity. Definition CRUD is not evidence that an invocation endpoint has been published. |
| Optional execution/storage workloads | Cross-workspace Lakehouse, custom pool/mount settings, DataWarehouse notebook, external-provider matrix and Dataflow/MLV/dbt schedule configurations are absent. Earlier tenant restrictions on shortcut transforms/cache remain documented in `feature-lanes.md`; they were not changed by these fixes. |
| Legacy mirrored layouts | Only the schema-enabled mirrored fixture is available. A real flattened legacy layout with mapped columns remains necessary; mocked paths and current Lakehouse column-mapping cases do not establish this service behavior. Microsoft describes the [legacy layout distinction](https://learn.microsoft.com/en-us/fabric/mirroring/troubleshooting#replicate-source-schema-hierarchy). |
| Calendar recurrence and DST | Disabled monthly/ordinal CRUD and ordinary-offset tests do not prove actual recurrence or the nonexistent/repeated local times at a DST transition. Those execution cases remain outstanding. |
| KQL cancellation and cleanup denial | No service-canceled ingestion fixture or valid restricted storage identity is configured. Existing recovery coverage injects its initial cleanup failure. Sending an invalid bearer would test authentication, not a real authorization denial. |

See [feature-lanes.md](feature-lanes.md) for the exact required feature names and
fixture contracts. Missing prerequisites fail their designated required lanes;
ordinary optional skips are not evidence that those paths work.
