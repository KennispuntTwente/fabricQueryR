# fabricQueryR (development version)

* `fabric_onelake_read_delta_table()` now preserves the full staged table layout
  and resolves snapshots from both JSON commits and Parquet checkpoints. It
  rejects unsupported Delta reader protocols, column mapping, and deletion
  vectors before returning data instead of risking incorrect results. The new
  `version` argument supports versioned reads.
  Directory entries returned by OneLake are excluded from file downloads.

* `fabric_pbi_dax_query()` now rejects embedded response, query, and table
  errors—including HTTP 200 partial-result responses—rather than returning
  incomplete data. It also supports direct `workspace_id`/`dataset_id` lookup
  bypass, optional RLS impersonation, paginated workspace lookup, and ambiguity
  errors for duplicate names.

* Authentication and REST behavior are now shared across Fabric surfaces.
  Exported functions accept refreshable `token_provider` callbacks in addition
  to static tokens and interactive `AzureAuth`; REST calls use bounded retries
  for throttling/transient failures, honor `Retry-After`, refresh after 401,
  and include redacted endpoint/request diagnostics. Shared pagination and
  Fabric long-running-operation polling helpers are covered by deterministic
  tests.

* `fabric_workspaces()`, `fabric_items()`, and `fabric_item()` now provide
  paginated, ambiguity-safe Fabric discovery. Typed helpers enrich Lakehouses,
  Warehouses, SQL Databases, semantic models, Eventhouses/KQL databases,
  notebooks, and GraphQL APIs with the workload properties available from
  Fabric. Discovery records expose ready-to-use SQL, OneLake, DAX, Livy, and KQL
  targets and can be passed directly to the corresponding query helpers.

# fabricQueryR 0.2.1

* Update e-mail address of maintainer in DESCRIPTION file (change to a 
personal e-mail address due to leaving the organization).

* `fabric_onelake_read_delta_table()`: add experimental support for specifying
a Lakehouse schema name to read from a specific schema within a Lakehouse which
has Lakehouse schemas enabled.

# fabricQueryR 0.2.0

* Added `fabric_livy_query()` to run queries against the 'Fabric Livy API',
allowing remote execution of 'Spark' code.

# fabricQueryR 0.1.1

* Initial CRAN submission.
