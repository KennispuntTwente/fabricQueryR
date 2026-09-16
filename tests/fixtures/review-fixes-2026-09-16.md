# Review fixes from baseline 625fb2bb

Each confirmed defect and the Arrow 9 compatibility failure has a separate
commit. `NEWS.md` and `README.md` are unchanged.

## Fixes and regression evidence

| Finding | Change | Evidence |
| --- | --- | --- |
| XMLA identity properties silently ignored | Reject `EffectiveUserName`, `Roles`, and `CustomData` in copied connection strings, including case/whitespace variants. Explain the dedicated REST arguments without exposing property values. | Unit tests cover JSON/Arrow, explicit impersonation, quoted/braced values, and rejection before token acquisition or lookup. Real RLS/impersonation remains dependent on the authorization matrix. |
| OneLake downloads decompress stored content | Disable curl HTTP content decoding for memory, file, and range downloads. | A real local HTTP server serves gzip bytes; byte-for-byte assertions pass. Live Fabric tests verify stored `Content-Encoding: gzip` and full, disk, and ranged bytes: four assertions. |
| GraphQL date-times lose offsets/fractions | Reject nested `POSIXct`/`POSIXlt` inputs and require explicit ISO 8601 strings. | Unit tests cover time zones, fractions, vectors, missing values, nested inputs, and both query/pagination entry points. Four live assertions verify a date-time filter against the fixture's actual returned timestamp and collected rows. |
| Failed staging create deletes an unrelated file | Definite HTTP client rejections relinquish cleanup ownership; ambiguous transport failures retain cleanup. | Unit tests cover seven HTTP rejection statuses and existing ambiguous-create behavior. Five live assertions force a collision, verify the original bytes survive, and confirm the destination was never created. |
| Livy loses zero-column row counts | Construct the tibble with the response's explicit row count. | Unit tests cover zero, one, and three rows through both MIME parsers. Three live assertions check a Spark view with three rows and no columns, and its empty result. |
| Arrow 9 decimal test fails | Construct decimal test values from doubles and compare collected values without unsupported string/decimal casts. Preserve decimal schema, null, order, timestamp, and unsupported-type assertions. | The CI filter passes using an isolated installation of **Arrow 9.0.0**, and with the regular installed Arrow version. |

The storage metadata behavior follows Microsoft's
[Path Update contract](https://learn.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/update),
which stores `x-ms-content-encoding` for subsequent reads. The test executes the
real HTTP transport because mocked responses do not exercise curl decoding.

## Additional live coverage

- Ordinary Livy statement cancellation and subsequent Spark execution in the
  same session: three assertions passed. The canceled work runs on an executor;
  a driver-side sleep would not establish reliable cancellation.
- Livy batch `environment_id`, `conf`, `args`, `py_files`, `files`, `jars`, and
  `archives`: three assertions passed. The application itself also asserts the
  published Environment's broadcast-timeout setting, the shuffle override,
  Python import, executor file/archive contents, and JVM JAR resource. A unique
  output marker proves this batch produced the result. The initial fixture
  incorrectly read a distributed file from the driver; it now verifies files
  and archives on executors.

Live probes use `connect_playground_sandbox()` to verify the persistent workspace
ownership marker and discover actual targets, then execute permanent test
bodies. They do not forge or certify the full seeded-fixture revision. The new
storage and Livy tests create their own files/views; the GraphQL probe obtains
the expected timestamp from the actual provisioned table. Temporary resources
are cleaned up by exact file/folder or session/batch handles.

## Validation commands

- Full offline `devtools::test(stop_on_failure = TRUE)` equivalent, including
  `FABRIC_DELTA_RS_ORACLE_TESTS=true` and the locked local Delta runtime:
  **7,477 passed assertions, zero failures/errors/test warnings, 171 skips**.
  Skips include disabled live tests, unavailable feature fixtures, and
  Windows-inapplicable cases.
- Arrow 9 CI filter: `^fabric_(arrow_staging|pbi_dax_query|warehouse_tables)$`.
- `uv --directory tools/fabric-sandbox run --locked pytest -q`: 211 tests passed.
- `air format .`, `jarl check .`, and `devtools::document()`: passed. Jarl's
  pre-existing loop-index warning was fixed by using a distinct index variable.
- `devtools::check(document = FALSE, manual = FALSE, args = "--no-tests")`,
  with child `LC_ALL=C`: zero errors, warnings, and notes. The full test suite is
  run separately above. `pkgdown::check_pkgdown()` also passed.

## Remaining integration evidence

These are not claimed as fixed by offline tests or by tests using broader
permissions. The existing required lanes in [feature-lanes.md](feature-lanes.md)
fail when their prerequisites are missing.

- Restricted identities, opposing OneLake grants, and semantic-model RLS,
  impersonation, roles, and customData still need the authorization matrix.
- The new required `livy-languages` lane exposes the Java/R batch gap with
  reproducible source fixtures and independent OneLake output checks. R attempts
  returned `Spark_User_SparkContext_DidNotInitialize`, including a retry with an
  explicit SparkR library path. Java did not produce its marker during the
  validation window. Neither is claimed as a successful execution test. The
  request fields follow the published
  [Fabric Livy OpenAPI schema](https://github.com/microsoft/fabric-samples/blob/main/docs-samples/data-engineering/Livy-API-swagger/swagger.yaml);
  a client-side cause has not been established.
- Enabled GraphQL introspection and the three published User Data Function
  endpoints remain unconfigured.
- Cross-workspace/custom-pool/mount/SQL-notebook options, external shortcut
  providers, workload-specific schedules, real canceled KQL ingestion and
  storage-denial identities, and a flattened legacy mirrored layout still need
  their dedicated fixtures.
- Non-UTC schedule firing remains unresolved after the earlier application and
  delegated runs described in [review-followup-validation.md](review-followup-validation.md).
  No client-side cause was established; enabled CRUD does not prove firing.
  Microsoft's [schedule contract](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/create-item-schedule)
  still specifies UTC boundaries. Actual monthly/ordinal recurrence and DST
  gap/fold execution remain unverified.
