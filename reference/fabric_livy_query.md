# Run a Livy API query (Spark code) in Microsoft Fabric

High-level helper that creates a Livy session in Microsoft Fabric, waits
for it to become idle, submits a statement with Spark code for
execution, retrieves the result, and closes the session.

## Usage

``` r
fabric_livy_query(
  livy_url,
  code,
  kind = c("spark", "pyspark", "sparkr", "sql"),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID", unset =
    "04b07795-8ddb-461a-bbee-02f9e1bf7b46"),
  access_token = NULL,
  environment_id = NULL,
  conf = NULL,
  verbose = TRUE,
  poll_interval = 2L,
  timeout = 600L
)
```

## Arguments

- livy_url:

  Character. Livy session job connection string, e.g.
  `"https://api.fabric.microsoft.com/v1/workspaces/.../lakehouses/.../livyapi/versions/2023-12-01/sessions"`
  (see details).

- code:

  Character. Code to run in the Livy session.

- kind:

  Character. One of `"spark"`, `"pyspark"`, `"sparkr"`, or `"sql"`.
  Indicates the type of Spark code being submitted for evaluation.

- tenant_id:

  Microsoft Azure tenant ID. Defaults to
  `Sys.getenv("FABRICQUERYR_TENANT_ID")` if missing.

- client_id:

  Microsoft Azure application (client) ID used to authenticate. Defaults
  to `Sys.getenv("FABRICQUERYR_CLIENT_ID")`. You may be able to use the
  Azure CLI app id `"04b07795-8ddb-461a-bbee-02f9e1bf7b46"`, but may
  want to make your own app registration in your tenant for better
  control.

- access_token:

  Optional character. If supplied, use this bearer token instead of
  acquiring a new one via `{AzureAuth}`.

- environment_id:

  Optional character. Fabric Environment (pool) ID to use for the
  session. If `NULL` (default), the default environment for the user
  will be used.

- conf:

  Optional list. Spark configuration settings to apply to the session.

- verbose:

  Logical. Emit progress via `{cli}`. Default `TRUE`.

- poll_interval:

  Integer. Polling interval in seconds when waiting for
  session/statement readiness.

- timeout:

  Integer. Timeout in seconds when waiting for session/statement
  readiness.

## Value

A list with statement details and results. The list contains:

- `id`: Statement ID.

- `state`: Final statement state (should be `"available"`).

- `started_local`: Local timestamp when statement started running.

- `completed_local`: Local timestamp when statement completed.

- `duration_sec`: Duration in seconds (local).

- `output`: A list with raw output details:

  - `status`: Output status (e.g., `"ok"`).

  - `execution_count`: Execution count (if applicable). The number of
    statements that have been executed in the session.

  - `data`: Raw data list with MIME types as keys (e.g. `"text/plain"`,
    `"application/json"`).

  - `parsed`: Parsed output, if possible. This may be a data frame
    (tibble) if the output was JSON tabular data, or a character vector
    if it was plain text. May be `NULL` if parsing was not possible.

- `url`: URL of the statement resource in the Livy API.

## Details

- In Microsoft Fabric, you can find and copy the Livy session URL by
  going to a 'Lakehouse' item, then go to 'Settings' -\> 'Livy Endpoint'
  -\> 'Session job connection string'.

- By default we request a token for
  `https://api.fabric.microsoft.com/.default`.

- AzureAuth is used to acquire the token. Be wary of caching behavior;
  you may want to call
  [`AzureAuth::clean_token_directory()`](https://rdrr.io/pkg/AzureAuth/man/get_azure_token.html)
  to clear cached tokens if you run into issues

## See also

[Livy API overview - Microsoft Fabric - 'What is the Livy API for Data
Engineering?'](https://learn.microsoft.com/en-us/fabric/data-engineering/api-livy-overview);
[Livy Docs - REST
API](https://livy.apache.org/docs/latest/rest-api.html).

## Examples

``` r
# Find your session URL in Fabric by going to a 'Lakehouse' item,
#   then go to 'Settings' -> 'Livy Endpoint' -> 'Session job connection string'
sess_url <- "https://api.fabric.microsoft.com/v1/workspaces/.../lakehouses/.../livyapi/..."

# Livy API can run SQL, SparkR, PySpark, & Spark
# Below are examples of 1) SQL & 2) SparkR usage

# Example is not executed since it requires configured credentials for Fabric
if (FALSE) { # \dontrun{
## 1 Livy & SQL

# Here we run SQL remotely in Microsoft Fabric with Spark, to get data to local R
# Since Livy API cannot directly give back a proper DF, we build it from returned schema & matrix

# Run Livy SQL query
livy_sql_result <- fabric_livy_query(
  livy_url = sess_url,
  kind = "sql",
  code = "SELECT * FROM Patienten LIMIT 1000",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)

# '$schema$fields' contains column info, & '$data' contains data as matrix without column names
payload <- livy_sql_result$output$data[["application/json"]]
schema  <- as_tibble(payload$schema$fields) # has columns: name, type, nullable
col_nms <- schema$name

# Build dataframe (tibble) from the Livy result
df_livy_sql <- payload$data |>
  as_tibble(.name_repair = "minimal") |>
  set_names(col_nms) |>
  mutate(
    # cast by schema$type (add more cases if your schema includes them)
    across(all_of(schema$name[schema$type == "long"]),    readr::parse_integer),
    across(all_of(schema$name[schema$type == "double"]),  readr::parse_double),
    across(all_of(schema$name[schema$type == "boolean"]), readr::parse_logical),
    across(all_of(schema$name[schema$type == "string"]),  as.character)
  )

## 2 Livy & SparkR

# Here we run R code remotely in Microsoft Fabric with SparkR, to get data to local R
# Since Livy API cannot directly give back a proper DF, we encode/decode B64 in SparkR/local R

# Run Livy SparkR query
livy_sparkr_result <- fabric_livy_query(
  livy_url = sess_url,
  kind = "sparkr",
  code = paste(
    # Obtain data in remote R (SparkR)
    'library(SparkR); library(base64enc)',
    'df <- sql("SELECT * FROM Patienten") |> limit(1000L) |> collect()',

    # serialize -> gzip -> base64
    'r_raw <- serialize(df, connection = NULL)',
    'raw_gz <- memCompress(r_raw, type = "gzip")',
    'b64 <- base64enc::base64encode(raw_gz)',

    # output marked B64 string
    'cat("<<B64RDS>>", b64, "<<END>>", sep = "")',
    sep = "\n"
  ),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)

# Extract marked B64 string from Livy output
txt <- livy_sparkr_result$output$data$`text/plain`
b64 <- sub('.*<<B64RDS>>', '', txt)
b64 <- sub('<<END>>.*', '', b64)

# Decode to dataframe
raw_gz <- base64enc::base64decode(b64)
r_raw  <- memDecompress(raw_gz, type = "gzip")
df_livy_sparkr <- unserialize(r_raw)
} # }
```
