# Queries use the same fixtures as playground/playground.R and the persistent CI
# sandbox. Only plain discovery records and fixed service tokens enter a worker.
playground_shiny_sources <- function() {
  data.frame(
    source = c(
      "warehouse",
      "lakehouse",
      "dax",
      "dax_arrow",
      "kql",
      "files",
      "delta",
      "mirrored",
      "graphql",
      "snapshot",
      "sql_database",
      "discovery"
    ),
    label = c(
      "Warehouse / SQL",
      "Lakehouse / SQL",
      "Semantic model / DAX",
      "Semantic model / DAX / Arrow",
      "Eventhouse / KQL",
      "OneLake / CSV",
      "OneLake / Delta",
      "Mirrored database / Delta",
      "GraphQL / paginated",
      "Warehouse snapshot / SQL",
      "SQL Database / SQL",
      "Workspace / discovery"
    ),
    target = c(
      "warehouse",
      "lakehouse",
      "semantic_model",
      "arrow_semantic_model",
      "kql_database",
      "lakehouse",
      "lakehouse",
      "mirrored_database",
      "graphql_api",
      "warehouse_snapshot",
      "sql_database",
      NA_character_
    ),
    service = c(
      "sql",
      "sql",
      "dax",
      "dax",
      "kql",
      "onelake",
      "onelake",
      "onelake",
      "graphql",
      "sql",
      "sql",
      "fabric"
    ),
    stringsAsFactors = FALSE
  )
}

playground_shiny_audience <- function(service, mode) {
  if (service == "graphql" && mode == "sandbox") {
    service <- "fabric"
  }
  c(
    fabric = "https://api.fabric.microsoft.com/.default",
    sql = "https://database.windows.net//.default",
    dax = "https://analysis.windows.net/powerbi/api/.default",
    graphql = "https://analysis.windows.net/powerbi/api/GraphQLApi.Execute.All",
    onelake = "https://storage.azure.com/.default",
    kql = "https://api.kusto.windows.net/.default"
  )[[service]]
}

playground_shiny_request <- function(
  source,
  workspace,
  targets,
  mode,
  category = "All",
  min_id = 1L,
  backend = "odbc"
) {
  sources <- playground_shiny_sources()
  source <- match.arg(source, sources$source)
  category <- match.arg(category, c("All", "A", "B"))
  backend <- match.arg(backend, c("odbc", "adbc"))
  if (
    length(min_id) != 1L ||
      !is.numeric(min_id) ||
      is.na(min_id) ||
      !is.finite(min_id) ||
      min_id < 1 ||
      min_id > 3 ||
      min_id != as.integer(min_id)
  ) {
    cli::cli_abort("Minimum ID must be an integer from 1 to 3.")
  }
  row <- sources[sources$source == source, ]
  target <- if (is.na(row$target)) NULL else targets[[row$target]]
  if (!is.na(row$target) && is.null(target)) {
    cli::cli_abort("The sandbox is missing the {.val {row$target}} fixture.")
  }
  statement <- switch(
    source,
    warehouse = ,
    snapshot = ,
    sql_database = ,
    lakehouse = paste0(
      "SELECT id, name, amount FROM dbo.",
      if (source == "lakehouse") {
        "fabricqueryr_basic"
      } else {
        "fabricqueryr_sql_types"
      },
      " WHERE id >= ? ORDER BY id"
    ),
    dax = ,
    dax_arrow = paste0(
      "EVALUATE\nSUMMARIZECOLUMNS(\n  'Facts'[category],\n",
      if (category != "All") {
        paste0("  TREATAS({\"", category, "\"}, 'Facts'[category]),\n")
      },
      "  \"Rows\", COUNTROWS('Facts'),\n  \"Amount\", SUM('Facts'[amount])\n)",
      "\nORDER BY 'Facts'[category]"
    ),
    kql = paste(
      "declare query_parameters(selected_category:string);",
      "fabricqueryr_events",
      "| where selected_category == 'All' or category == selected_category",
      "| project id, name, category, amount | order by id asc",
      sep = "\n"
    ),
    files = "fabric_onelake_read_file(workspace, lakehouse, path = 'Files/fixtures/basic.csv', token = token)",
    delta = "fabric_lakehouse_read_table(lakehouse, 'fabricqueryr_basic', schema = 'dbo', token = token)",
    mirrored = "fabric_mirrored_database_read_table(mirrored_database, 'fabricqueryr_mirror_types', schema = 'dbo', token = token)",
    graphql = paste(
      "query Paged($first: Int!, $after: String) {",
      "  fabricqueryr_basics(first: $first, after: $after, orderBy: {id: ASC}) {",
      "    items { id name category amount }",
      "    hasNextPage endCursor",
      "  }",
      "}",
      sep = "\n"
    ),
    discovery = "fabric_items(workspace, detail = FALSE, token = token)"
  )
  list(
    source = source,
    label = row$label,
    service = row$service,
    workspace = workspace,
    target = target,
    statement = statement,
    category = if (row$service %in% c("dax", "kql")) category else NULL,
    min_id = if (row$service == "sql") as.integer(min_id) else NULL,
    backend = if (row$service == "sql") backend else NULL,
    audience = playground_shiny_audience(row$service, mode)
  )
}

# This function is also usable directly from an R session, without Shiny.
playground_shiny_read <- function(request, token) {
  target <- request$target
  statement <- request$statement
  switch(
    request$source,
    warehouse = ,
    lakehouse = ,
    snapshot = ,
    sql_database = fabricQueryR::fabric_sql_query(
      target,
      sql = statement,
      params = list(request$min_id),
      backend = request$backend,
      numeric_policy = "driver",
      token = token,
      read_only = TRUE,
      verbose = FALSE
    ),
    dax = ,
    dax_arrow = {
      data <- fabricQueryR::fabric_pbi_dax_query(
        target,
        dax = statement,
        api = if (request$source == "dax_arrow") "arrow" else "json",
        token = token
      )
      names(data) <- c("Category", "Rows", "Amount")
      data
    },
    kql = fabricQueryR::fabric_kql_query(
      target,
      query = statement,
      parameters = list(selected_category = request$category),
      token = token
    ),
    files = fabricQueryR::fabric_onelake_read_file(
      request$workspace,
      target,
      path = "Files/fixtures/basic.csv",
      token = token
    ),
    delta = fabricQueryR::fabric_lakehouse_read_table(
      target,
      table = "fabricqueryr_basic",
      schema = "dbo",
      columns = c("id", "name", "category", "amount"),
      limit = 100L,
      token = token,
      verbose = FALSE
    ),
    mirrored = fabricQueryR::fabric_mirrored_database_read_table(
      target,
      table = "fabricqueryr_mirror_types",
      schema = "dbo",
      columns = c("id", "name", "amount"),
      limit = 100L,
      token = token,
      verbose = FALSE
    ),
    graphql = {
      pages <- fabricQueryR::fabric_graphql_paginate(
        target,
        query = statement,
        variables = list(first = 2L, after = NULL),
        operation_name = "Paged",
        next_cursor = fabricQueryR::fabric_graphql_cursor(
          "fabricqueryr_basics"
        ),
        error_policy = "error",
        token = token,
        audience = request$audience
      )
      data <- fabricQueryR::fabric_graphql_collect(
        pages,
        c("fabricqueryr_basics", "items")
      )
      attr(data, "pages") <- length(pages$pages)
      data
    },
    discovery = {
      items <- fabricQueryR::fabric_items(
        request$workspace,
        detail = FALSE,
        token = token
      )
      data.frame(
        Name = vapply(
          items,
          function(item) item[["displayName"]],
          character(1)
        ),
        Type = vapply(items, function(item) item[["type"]], character(1))
      )
    }
  )
}

playground_shiny_run <- function(requests, tokens) {
  lapply(requests, function(request) {
    start <- Sys.time()
    result <- tryCatch(
      {
        token <- tokens[[request$service]]
        if (inherits(token, "error")) {
          stop(token)
        }
        data <- playground_shiny_read(request, token)
        list(ok = TRUE, data = data, error = NULL)
      },
      error = function(error) {
        list(ok = FALSE, data = NULL, error = conditionMessage(error))
      }
    )
    c(
      result,
      list(
        request = request,
        seconds = as.numeric(difftime(Sys.time(), start, units = "secs")),
        pid = Sys.getpid()
      )
    )
  })
}
