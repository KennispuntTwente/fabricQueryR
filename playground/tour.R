# Interactive fabricQueryR tour ----
# Open this file and run one section at a time from the repository root.
# Wait for the persistent sandbox deployment to finish before connecting.
# Sections 1-7 read data; optional blocks are disabled with if (FALSE).
# Source playground.R for reusable demos without running any of them.

# 1. Connect and discover ----
source("playground/sandbox.R")
source("playground/playground.R")
sandbox <- connect_playground_sandbox()

inventory <- demo_discovery(sandbox)$inventory
inventory
names(Filter(Negate(is.null), sandbox$targets))

# Discovered items are R6 objects carrying authentication and workspace details.
# Type `warehouse$` in your editor to explore its available methods.
warehouse <- sandbox$targets$warehouse
lakehouse <- sandbox$targets$lakehouse
warehouse
lakehouse

# 2. SQL: discover, select, and parameterize ----
sql_tables <- warehouse$sql_tables(schema = "dbo")
sql_tables
warehouse$sql_views(schema = "dbo")

sql_rows <- warehouse$read_table(
  "fabricqueryr_sql_types",
  schema = "dbo",
  columns = c("id", "name", "category", "amount"),
  limit = 3L
)
sql_rows
# Seeded rows: alpha / A / 10.5, beta / B / 20, gamma / A / NA.
# Table reads have no guaranteed order. Use ORDER BY when order matters.
selected <- warehouse$sql_query(
  paste(
    "SELECT id, name, amount FROM dbo.fabricqueryr_sql_types",
    "WHERE category = ? ORDER BY id"
  ),
  params = list("A")
)
selected # alpha and gamma

# The functional API works with the same discovered object.
fabric_sql_read_table(
  warehouse,
  table = "fabricqueryr_sql_types",
  columns = c("id", "name"),
  limit = 3L,
  token = sandbox$token
)

# ODBC defaults to driver conversion and warns once per R session about precision.
# "driver" explicitly accepts that conversion; "exact" rejects unsafe ODBC types.
# Optional ADBC preserves decimals as strings and large integers exactly.
if (FALSE) {
  precise <- warehouse$sql_query(
    paste(
      "SELECT CAST('12345678901234567890.1234' AS decimal(24,4)) AS amount,",
      "CAST('9007199254740993' AS bigint) AS large"
    ),
    backend = "adbc"
  )
  precise$amount
  as.character(precise$large)
  demo_sql(sandbox, backend = "adbc", targets = "warehouse")
}

# Read the same API across Lakehouse SQL, Warehouse, and its read-only snapshot.
# An optional SQL Database is skipped if deployment omitted it.
sql_surfaces <- demo_sql(sandbox)
names(sql_surfaces)
sql_surfaces$warehouse$summary # row_count = 3; amount_sum = 30.5

# 3. Reuse a DBI connection ----
# local() supplies a function scope so on.exit() runs even if a query errors.
dbi_rows <- local({
  con <- open_playground_sql_connection(sandbox, target = "warehouse")
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbGetQuery(
    con,
    "SELECT id, name FROM dbo.fabricqueryr_sql_types WHERE category = ? ORDER BY id",
    params = list("A")
  )
})
dbi_rows

# 4. Arrow: consume batches, then release the stream ----
batch_summary <- demo_arrow_batches(sandbox)
batch_summary # rows = 3; amount_sum = 30.5; batch count depends on the driver

# The same pattern works with OneLake and DAX streams. Never reuse a consumed
# stream; run the query again. read_table() on an Arrow reader collects all
# remaining batches, whereas read_next_batch() bounds each processing step.
stream_rows <- local({
  stream <- warehouse$sql_query(
    "SELECT id, name FROM dbo.fabricqueryr_sql_types ORDER BY id",
    result = "arrow_stream"
  )
  on.exit(nanoarrow::nanoarrow_pointer_release(stream), add = TRUE)
  reader <- arrow::as_record_batch_reader(stream)
  on.exit(reader$Close(), add = TRUE, after = FALSE)
  count <- 0
  repeat {
    batch <- reader$read_next_batch()
    if (is.null(batch)) {
      break
    }
    print(as.data.frame(batch))
    count <- count + batch$num_rows
  }
  count
})
stopifnot(stream_rows == 3)

# 5. OneLake files and Delta tables ----
lakehouse$onelake_list(path = "Files/fixtures", recursive = TRUE)
csv <- lakehouse$onelake_read_file("Files/fixtures/basic.csv")
csv
lakehouse$onelake_metadata("Files/fixtures/basic.csv")
lakehouse$schemas()
lakehouse$tables()

# Direct table reads use Delta through Python, not the Lakehouse SQL endpoint.
# The first read may install the managed Python dependencies.
fabric_delta_config()
delta <- lakehouse$read_table(
  "fabricqueryr_basic",
  schema = "dbo",
  columns = c("id", "name", "amount"),
  limit = 3L
)
delta
stopifnot(nrow(csv) == 3L, nrow(delta) == 3L)

# Also exercise the mirrored database and its published OneLake tables.
onelake <- demo_onelake(sandbox)
onelake$mirrored_rows

# 6. KQL and GraphQL ----
kql <- sandbox$targets$kql_database
kql$tables()
kql$query(
  paste(
    "declare query_parameters(selected_category:string);",
    "fabricqueryr_events | where category == selected_category | order by id asc"
  ),
  parameters = list(selected_category = "A")
)
demo_kql(sandbox)$selected

# The demo pages through three rows with page size two and collects items.
# Open demo_graphql() in playground.R to edit the query and variables.
api <- sandbox$targets$graphql_api
page <- api$query(paste(
  "query { fabricqueryr_basics(first: 2, orderBy: {id: ASC}) {",
  "items { id name amount } hasNextPage endCursor } }"
))
page$data$fabricqueryr_basics
graphql <- demo_graphql(sandbox)
graphql$rows
length(graphql$pages$pages) # 2 pages

# 7. Power BI: DAX over JSON and Arrow ----
models <- demo_power_bi(sandbox)
models$json
models$arrow
sandbox$targets$arrow_semantic_model$dax_query(
  'EVALUATE ROW("rows", COUNTROWS(\'Facts\'))',
  api = "arrow"
)

# Optional: catalog search (preview API, requires Catalog.Read.All).
# Search visibility and indexing can differ from direct workspace discovery.
if (FALSE) {
  catalog <- fabric_catalog_search(
    search = "TestLakehouse",
    types = "Lakehouse",
    token = sandbox$token
  )
  catalog
}

# 8. Writes: temporary objects with automatic cleanup ----
# Run individual calls deliberately. These upload data or create objects.
if (FALSE) {
  demo_onelake_write(sandbox)$rows
  demo_onelake_shortcut(sandbox)$inspected
  demo_warehouse_write(sandbox)$rows
}

# A dedicated Lakehouse table is retained after this call (no table-delete API).
if (FALSE) {
  written <- write_playground_lakehouse_table(sandbox)
  written$rows
}

# KQL ingestion leaves a dedicated table too. Keep the ingestion key stable
# when retrying this same batch; use a new key for a different batch of data.
if (FALSE) {
  ingested <- kql$write_table(
    table = "fabricqueryr_playground_events",
    data = data.frame(id = 1:3, label = c("first", "second", "third")),
    create_if_missing = TRUE,
    ingest_if_not_exists = "fabricqueryr-playground-events-v1",
    skip_batching = TRUE,
    timeout = 600
  )
  ingested$status$state
  kql$query("fabricqueryr_playground_events | order by id asc")
}

# Export runs in Fabric and writes Parquet files to OneLake. A unique directory
# is removed on exit after reading the exported data back into R.
if (FALSE) {
  exported_rows <- local({
    path <- paste0("Files/playground/", basename(tempfile("kql-export-")))
    on.exit(
      try(lakehouse$onelake_delete(path, recursive = TRUE, confirm = TRUE)),
      add = TRUE
    )
    exported <- kql$export(
      "fabricqueryr_events | project id, name, amount",
      destination = lakehouse,
      path = path,
      format = "parquet",
      timeout = 600
    )
    exported$state
    files <- lakehouse$onelake_list(path, recursive = TRUE)
    parquet <- files$path[
      !files$is_directory & grepl("[.]parquet$", files$path)
    ]
    dplyr::bind_rows(lapply(parquet, function(file) {
      lakehouse$onelake_read_file(file, format = "parquet")
    }))
  })
  exported_rows
}

# 9. Jobs, refreshes, and Spark ----
# Inspect history without starting compute.
if (FALSE) {
  demo_job_history(sandbox)
  fabric_job_schedules(sandbox$targets$pipeline, token = sandbox$token)
}

# These start work and may take several minutes. Run one at a time.
if (FALSE) {
  # Uses the Import model; the JSON demo's Push model cannot be refreshed.
  refresh <- demo_power_bi_refresh(sandbox)
  refresh$completed
  refresh$history

  job <- run_playground_job(sandbox, target = "pipeline", wait = FALSE)
  fabric_job_status(job)
  fabric_job_wait(job, timeout = 1200, cancel_on_timeout = TRUE)
  # To stop a job you started: fabric_job_cancel(job)

  run_playground_job(sandbox, target = "job_notebook")
  run_playground_job(sandbox, target = "spark_job")

  # Uses the sign-in method chosen by connect_playground_sandbox(), and closes
  # its temporary Spark session after the SQL query completes.
  spark <- demo_livy(sandbox)
  spark$parsed
}
