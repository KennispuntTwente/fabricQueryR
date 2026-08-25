# Interactive examples for the persistent Fabric sandbox
#
# Source this file to define the demos, then run one function at a time

playground_require_sandbox <- function(sandbox) {
  if (!inherits(sandbox, "fabricqueryr_playground_sandbox")) {
    cli::cli_abort(
      "{.arg sandbox} must be returned by {.fn connect_playground_sandbox}"
    )
  }
  invisible(sandbox)
}

playground_table_row <- function(tables, name, schema = NULL) {
  matches <- tables$name == name
  if (!is.null(schema) && "schema" %in% names(tables)) {
    matches <- matches & tables$schema == schema
  }
  matches[is.na(matches)] <- FALSE
  if (sum(matches) != 1L) {
    cli::cli_abort(
      "Expected one table named {.val {name}} but found {sum(matches)}"
    )
  }
  tables[matches, , drop = FALSE]
}

# Discover the workspace, its item inventory, and the main typed resources
demo_discovery <- function(sandbox) {
  playground_require_sandbox(sandbox)

  inventory <- data.frame(
    display_name = vapply(
      sandbox$items,
      playground_item_display_name,
      character(1)
    ),
    type = vapply(
      sandbox$items,
      function(item) item[["type"]],
      character(1)
    ),
    id = vapply(
      sandbox$items,
      function(item) item[["id"]],
      character(1)
    ),
    row.names = NULL
  )
  inventory <- inventory[order(inventory$type, inventory$display_name), ]

  list(
    workspace = sandbox$workspace[c("displayName", "id", "description")],
    inventory = inventory,
    lakehouses = fabric_lakehouses(
      sandbox$workspace,
      token = sandbox$token
    ),
    warehouses = fabric_warehouses(
      sandbox$workspace,
      token = sandbox$token
    ),
    kql_databases = fabric_kql_databases(
      sandbox$workspace,
      token = sandbox$token
    ),
    semantic_models = fabric_semantic_models(
      sandbox$workspace,
      token = sandbox$token
    )
  )
}

# Query every seeded SQL surface through the package's one-shot helpers
demo_sql <- function(sandbox, backend = c("odbc", "adbc")) {
  playground_require_sandbox(sandbox)
  backend <- match.arg(backend)

  cases <- list(
    lakehouse = list(
      target = sandbox$targets$lakehouse,
      table = "fabricqueryr_basic"
    ),
    warehouse = list(
      target = sandbox$targets$warehouse,
      table = "fabricqueryr_sql_types"
    ),
    warehouse_snapshot = list(
      target = sandbox$targets$warehouse_snapshot,
      table = "fabricqueryr_sql_types"
    ),
    sql_database = list(
      target = sandbox$targets$sql_database,
      table = "fabricqueryr_sql_types"
    )
  )

  lapply(cases, function(case) {
    tables <- fabric_sql_tables(
      case$target,
      schema = "dbo",
      backend = backend,
      token = sandbox$token
    )
    rows <- fabric_sql_read_table(
      case$target,
      table = case$table,
      schema = "dbo",
      columns = c("id", "name", "amount"),
      limit = 3L,
      backend = backend,
      token = sandbox$token
    )
    summary <- fabric_sql_query(
      case$target,
      sql = paste(
        "SELECT COUNT_BIG(*) AS row_count,",
        "SUM(amount) AS amount_sum",
        paste0("FROM dbo.[", case$table, "]")
      ),
      backend = backend,
      token = sandbox$token
    )

    list(
      connection = fabric_sql_connection_info(case$target),
      tables = tables,
      rows = rows,
      summary = summary
    )
  })
}

# Open a reusable DBI connection for ad hoc interactive SQL
open_playground_sql_connection <- function(
  sandbox,
  target = c("warehouse", "lakehouse", "sql_database"),
  backend = c("odbc", "adbc")
) {
  playground_require_sandbox(sandbox)
  target <- match.arg(target)
  backend <- match.arg(backend)

  fabric_sql_connect(
    sandbox$targets[[target]],
    backend = backend,
    token = sandbox$token
  )
}

# Read seeded files, Delta tables, and a mirrored table through OneLake
demo_onelake <- function(sandbox) {
  playground_require_sandbox(sandbox)
  workspace <- sandbox$workspace
  lakehouse <- sandbox$targets$lakehouse
  mirrored <- sandbox$targets$mirrored_database

  files <- fabric_onelake_list(
    workspace,
    lakehouse,
    path = "Files/fixtures",
    recursive = TRUE,
    token = sandbox$token
  )
  csv <- fabric_onelake_read_file(
    workspace,
    lakehouse,
    path = "Files/fixtures/basic.csv",
    token = sandbox$token
  )

  lakehouse_tables <- fabric_lakehouse_tables(
    lakehouse,
    token = sandbox$token
  )
  basic <- playground_table_row(
    lakehouse_tables,
    "fabricqueryr_basic",
    schema = "dbo"
  )
  delta <- fabric_lakehouse_read_table(
    lakehouse,
    basic,
    columns = c("id", "name", "amount"),
    limit = 3L,
    token = sandbox$token
  )

  mirrored_tables <- fabric_mirrored_database_tables(
    mirrored,
    token = sandbox$token
  )
  mirrored_types <- playground_table_row(
    mirrored_tables,
    "fabricqueryr_mirror_types",
    schema = "dbo"
  )
  mirrored_rows <- fabric_mirrored_database_read_table(
    mirrored,
    mirrored_types,
    columns = c("id", "name", "amount"),
    limit = 3L,
    token = sandbox$token
  )

  list(
    delta_config = fabric_delta_config(),
    files = files,
    csv = csv,
    lakehouse_tables = lakehouse_tables,
    delta = delta,
    mirrored_tables = mirrored_tables,
    mirrored_rows = mirrored_rows
  )
}

# Round-trip a temporary CSV file and remove it before returning
demo_onelake_write <- function(sandbox) {
  playground_require_sandbox(sandbox)
  workspace <- sandbox$workspace
  lakehouse <- sandbox$targets$lakehouse
  path <- paste0(
    "Files/playground/fabricqueryr-demo-",
    format(Sys.time(), "%Y%m%d%H%M%S", tz = "UTC"),
    "-",
    Sys.getpid(),
    ".csv"
  )
  on.exit(
    try(
      fabric_onelake_delete(
        workspace,
        lakehouse,
        path = path,
        confirm = TRUE,
        token = sandbox$token
      ),
      silent = TRUE
    ),
    add = TRUE
  )

  data <- data.frame(
    id = 1:3,
    label = c("first", "second", "third")
  )
  written <- fabric_onelake_write_file(
    workspace,
    lakehouse,
    path = path,
    data = data,
    overwrite = TRUE,
    token = sandbox$token
  )

  list(
    path = path,
    written = written,
    metadata = fabric_onelake_metadata(
      workspace,
      lakehouse,
      path = path,
      token = sandbox$token
    ),
    rows = fabric_onelake_read_file(
      workspace,
      lakehouse,
      path = path,
      token = sandbox$token
    )
  )
}

# Create, inspect, and remove a same-Lakehouse OneLake shortcut
demo_onelake_shortcut <- function(sandbox) {
  playground_require_sandbox(sandbox)
  lakehouse <- sandbox$targets$lakehouse
  name <- paste0("fabricqueryr_playground_", Sys.getpid())
  created <- FALSE
  on.exit(
    if (created) {
      try(
        fabric_onelake_shortcut_delete(
          lakehouse,
          path = "Files",
          name = name,
          confirm = TRUE,
          token = sandbox$token
        ),
        silent = TRUE
      )
    },
    add = TRUE
  )

  shortcut <- fabric_onelake_shortcut_create(
    lakehouse,
    path = "Files",
    name = name,
    target = lakehouse,
    target_path = "Files/fixtures/nested",
    conflict_policy = "CreateOrOverwrite",
    token = sandbox$token
  )
  created <- TRUE

  list(
    created = shortcut,
    inspected = fabric_onelake_shortcut_get(
      lakehouse,
      path = "Files",
      name = name,
      token = sandbox$token
    ),
    listed = fabric_onelake_shortcuts(
      lakehouse,
      parent_path = "Files",
      token = sandbox$token
    )
  )
}

# Write and read a temporary Warehouse table, then drop it before returning
demo_warehouse_write <- function(
  sandbox,
  backend = c("odbc", "adbc")
) {
  playground_require_sandbox(sandbox)
  backend <- match.arg(backend)
  warehouse <- sandbox$targets$warehouse
  table <- paste0(
    "fabricqueryr_playground_",
    format(Sys.time(), "%Y%m%d%H%M%S", tz = "UTC"),
    "_",
    Sys.getpid()
  )
  con <- fabric_sql_connect(
    warehouse,
    backend = backend,
    token = sandbox$token
  )
  on.exit(
    {
      try(
        DBI::dbExecute(
          con,
          paste0("DROP TABLE [dbo].[", table, "]")
        ),
        silent = TRUE
      )
      try(DBI::dbDisconnect(con), silent = TRUE)
    },
    add = TRUE
  )

  data <- data.frame(
    id = 1:3,
    label = c("first", "second", "third"),
    amount = c(10.5, 20, NA_real_)
  )
  written <- fabric_warehouse_write_table(
    warehouse,
    table = table,
    data = data,
    staging_lakehouse = sandbox$targets$lakehouse,
    create_if_missing = TRUE,
    mode = "Append",
    backend = backend,
    token = sandbox$token
  )

  list(
    table = table,
    written = written,
    rows = fabric_warehouse_read_table(
      warehouse,
      table,
      backend = backend,
      token = sandbox$token
    )
  )
}

# Create or replace a dedicated Lakehouse demo table
write_playground_lakehouse_table <- function(
  sandbox,
  table = "fabricqueryr_playground_orders"
) {
  playground_require_sandbox(sandbox)
  orders <- data.frame(
    order_id = 1:3,
    order_date = as.Date(c("2026-08-23", "2026-08-24", "2026-08-25")),
    amount = c(10.5, 20, 30.25)
  )
  written <- fabric_lakehouse_write_table(
    sandbox$targets$lakehouse,
    table = table,
    data = orders,
    mode = "Overwrite",
    token = sandbox$token
  )

  list(
    table = table,
    written = written,
    rows = fabric_lakehouse_read_table(
      sandbox$targets$lakehouse,
      table = table,
      token = sandbox$token
    )
  )
}

# List and query the seeded KQL database with typed parameters
demo_kql <- function(sandbox) {
  playground_require_sandbox(sandbox)
  database <- sandbox$targets$kql_database

  tables <- fabric_kql_tables(database, token = sandbox$token)
  events <- playground_table_row(tables, "fabricqueryr_events")
  rows <- fabric_kql_read_table(
    database,
    events,
    columns = c("id", "name", "category", "amount"),
    limit = 3L,
    token = sandbox$token
  )
  selected <- fabric_kql_query(
    database,
    query = paste(
      "declare query_parameters(selected_category:string);",
      "fabricqueryr_events",
      "| where category == selected_category",
      "| order by id asc"
    ),
    parameters = list(selected_category = "A"),
    token = sandbox$token
  )

  list(tables = tables, rows = rows, selected = selected)
}

# Query, paginate, and optionally introspect the seeded GraphQL API
demo_graphql <- function(sandbox) {
  playground_require_sandbox(sandbox)
  api <- sandbox$targets$graphql_api
  root <- "fabricqueryr_basics"
  query <- paste(
    "query Paged($first: Int!, $after: String) {",
    paste0("  ", root, "("),
    "    first: $first, after: $after, orderBy: {id: ASC}",
    "  ) {",
    "    items { id name category amount loaded_at }",
    "    hasNextPage",
    "    endCursor",
    "  }",
    "}"
  )
  pages <- fabric_graphql_paginate(
    api,
    query = query,
    variables = list(first = 2L, after = NULL),
    operation_name = "Paged",
    next_cursor = fabric_graphql_cursor(root),
    error_policy = "error",
    token = sandbox$token,
    audience = "https://api.fabric.microsoft.com/.default"
  )
  schema <- tryCatch(
    fabric_graphql_schema(
      api,
      token = sandbox$token,
      audience = "https://api.fabric.microsoft.com/.default"
    ),
    fabric_graphql_introspection_error = identity
  )

  list(
    pages = pages,
    rows = fabric_graphql_collect(pages, c(root, "items")),
    schema = schema
  )
}

# Query both seeded semantic models with JSON and Arrow DAX transports
demo_power_bi <- function(sandbox) {
  playground_require_sandbox(sandbox)
  query <- paste(
    "EVALUATE",
    "SELECTCOLUMNS(",
    "  'Facts',",
    "  \"id\", 'Facts'[id],",
    "  \"name\", 'Facts'[name],",
    "  \"category\", 'Facts'[category],",
    "  \"amount\", 'Facts'[amount]",
    ")",
    "ORDER BY [id]",
    sep = "\n"
  )

  list(
    json = fabric_pbi_dax_query(
      sandbox$targets$semantic_model,
      dax = query,
      token = sandbox$token
    ),
    arrow = fabric_pbi_dax_query(
      sandbox$targets$arrow_semantic_model,
      dax = query,
      api = "arrow",
      token = sandbox$token
    )
  )
}

# Submit a semantic-model refresh and wait for its terminal state
demo_power_bi_refresh <- function(sandbox) {
  playground_require_sandbox(sandbox)
  model <- sandbox$targets$semantic_model
  refresh <- fabric_pbi_refresh(model, token = sandbox$token)
  completed <- fabric_pbi_refresh_wait(
    refresh,
    timeout = 900,
    cancel_on_timeout = TRUE
  )

  list(
    completed = completed,
    history = fabric_pbi_refresh_history(
      model,
      top = 5L,
      token = sandbox$token
    )
  )
}

# Run one Spark SQL statement through a temporary Livy session
demo_livy <- function(sandbox) {
  playground_require_sandbox(sandbox)
  workspace <- sandbox$workspace
  lakehouse <- sandbox$targets$lakehouse
  table <- paste(
    sprintf(
      "`%s`",
      c(
        workspace$displayName,
        lakehouse$displayName,
        "dbo",
        "fabricqueryr_basic"
      )
    ),
    collapse = "."
  )

  result <- sandbox$livy$query(
    livy_url = lakehouse,
    kind = "sql",
    code = paste(
      "SELECT category, COUNT(*) AS row_count",
      paste("FROM", table),
      "GROUP BY category ORDER BY category"
    ),
    timeout = 900
  )

  list(result = result, parsed = result$output$parsed)
}

# Inspect recent runs without starting a new Fabric job
demo_job_history <- function(sandbox) {
  playground_require_sandbox(sandbox)

  list(
    notebook = fabric_job_instances(
      sandbox$targets$job_notebook,
      token = sandbox$token
    ),
    pipeline = fabric_job_instances(
      sandbox$targets$pipeline,
      token = sandbox$token
    ),
    spark_job = fabric_job_instances(
      sandbox$targets$spark_job,
      token = sandbox$token
    )
  )
}

# Start one deterministic sandbox job and optionally wait for completion
run_playground_job <- function(
  sandbox,
  target = c("pipeline", "spark_job", "job_notebook"),
  wait = TRUE
) {
  playground_require_sandbox(sandbox)
  target <- match.arg(target)
  if (!is.logical(wait) || length(wait) != 1L || is.na(wait)) {
    cli::cli_abort("{.arg wait} must be TRUE or FALSE")
  }

  arguments <- list(
    item = sandbox$targets[[target]],
    token = sandbox$token
  )
  if (identical(target, "job_notebook")) {
    arguments$parameters <- list(mode = "success", marker = "playground")
    arguments$default_lakehouse <- sandbox$targets$lakehouse$id
  }
  job <- do.call(fabric_job_run, arguments)
  if (!wait) {
    return(job)
  }

  fabric_job_wait(
    job,
    timeout = 1200,
    cancel_on_timeout = TRUE,
    notebook_details = identical(target, "job_notebook")
  )
}
