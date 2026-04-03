# Read a Microsoft Fabric/OneLake Delta table (ADLS Gen2)

Authenticates to OneLake (ADLS Gen2), resolves the table's `_delta_log`
to determine the *current* active Parquet parts, downloads only those
parts to a local staging directory, and returns the result as a tibble.

## Usage

``` r
fabric_onelake_read_delta_table(
  table_path,
  workspace_name,
  lakehouse_name,
  schema = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  dest_dir = NULL,
  verbose = TRUE,
  dfs_base = "https://onelake.dfs.fabric.microsoft.com"
)
```

## Arguments

- table_path:

  Character. Table name or nested path (e.g. `"Patienten"` or
  `"Patienten/patienten_hash"`). Only the last path segment is used as
  the table directory under `Tables/`.

- workspace_name:

  Character. Fabric workspace display name or GUID (this is the ADLS
  filesystem/container name).

- lakehouse_name:

  Character. Lakehouse item name, with or without the `.Lakehouse`
  suffix (e.g. `"Lakehouse"` or `"Lakehouse.Lakehouse"`).

- schema:

  Character or `NULL`. Lakehouse schema name (e.g. `"dbo"`). When
  supplied, the table is resolved under `Tables/<schema>/<table>`
  instead of `Tables/<table>`. Schema support requires a schema-enabled
  Lakehouse (enabled by default for new lakehouses). Defaults to `NULL`
  (no schema, for non-schema lakehouses). (Note: schema support through
  this argument is experimental.)

- tenant_id:

  Character. Entra ID (Azure AD) tenant GUID. Defaults to
  `Sys.getenv("FABRICQUERYR_TENANT_ID")` if missing.

- client_id:

  Character. App registration (client) ID. Defaults to
  `Sys.getenv("FABRICQUERYR_CLIENT_ID")`, falling back to the Azure CLI
  app id `"04b07795-8ddb-461a-bbee-02f9e1bf7b46"` if not set.

- dest_dir:

  Character or `NULL`. Local staging directory for Parquet parts. If
  `NULL` (default), a temp dir is used and cleaned up on exit.

- verbose:

  Logical. Print progress messages via `{cli}`. Default `TRUE`.

- dfs_base:

  Character. OneLake DFS endpoint. Default
  `"https://onelake.dfs.fabric.microsoft.com"`.

## Value

A tibble with the table's current rows (0 rows if the table is empty).

## Details

- In Microsoft Fabric, OneLake exposes each workspace as an ADLS Gen2
  filesystem. Within a Lakehouse item, Delta tables are stored under
  `Tables/<table>` (non-schema lakehouse) or `Tables/<schema>/<table>`
  (schema-enabled lakehouse) with a `_delta_log/` directory that tracks
  commit state. This helper replays the JSON commits to avoid
  double-counting compacted/removed files.

- Schema-enabled lakehouses (the default for new lakehouses) organise
  tables into named schemas. Supply the `schema` argument (e.g. `"dbo"`)
  to read a table stored under a specific schema.

- Ensure the account/principal you authenticate with has access via
  **Lakehouse -\> Manage OneLake data access** (or is a member of the
  workspace).

- AzureAuth is used to acquire the token. Be wary of caching behavior;
  you may want to call
  [`AzureAuth::clean_token_directory()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  to clear cached tokens if you run into issues

## Examples

``` r
# Example is not executed since it requires configured credentials for Fabric
if (FALSE) { # \dontrun{
df <- fabric_onelake_read_delta_table(
  table_path     = "Patients/PatientInfo",
  workspace_name = "PatientsWorkspace",
  lakehouse_name = "Lakehouse.Lakehouse",
  tenant_id      = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id      = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)
dplyr::glimpse(df)

# Schema-enabled lakehouse: read from Tables/dbo/PatientInfo
df2 <- fabric_onelake_read_delta_table(
  table_path     = "PatientInfo",
  workspace_name = "PatientsWorkspace",
  lakehouse_name = "Lakehouse.Lakehouse",
  schema         = "dbo"
)
} # }
```
