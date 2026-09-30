library(shiny)
library(fabricQueryR)

use_mirai <- requireNamespace("mirai", quietly = TRUE)
if (use_mirai) {
  mirai::daemons(2)
  onStop(\() mirai::daemons(0), session = NULL)
} else {
  future::plan(future::multisession, workers = 2)
  onStop(\() future::plan(future::sequential), session = NULL)
}

# Keep the sources your app needs.
sources <- c(
  "Semantic model (DAX)" = "dax",
  "Warehouse (SQL)" = "warehouse",
  "Lakehouse (SQL)" = "lakehouse",
  "Eventhouse (KQL)" = "kql",
  "OneLake file" = "files",
  "OneLake Delta table" = "delta",
  "GraphQL API" = "graphql"
)
service_for <- c(
  dax = "dax",
  warehouse = "sql",
  lakehouse = "sql",
  kql = "kql",
  files = "onelake",
  delta = "onelake",
  graphql = "graphql"
)
config <- fabric_shiny_config(
  tenant_id = Sys.getenv("ENTRA_TENANT_ID"),
  client_id = Sys.getenv("ENTRA_CLIENT_ID"),
  client_secret = Sys.getenv("ENTRA_CLIENT_SECRET"),
  redirect_uri = Sys.getenv("ENTRA_REDIRECT_URI", "http://localhost:8100/"),
  services = unique(unname(service_for[sources]))
)

# Replace the example table, column and measure names with your own.
read_data <- function(source, token, year) {
  switch(
    source,
    dax = {
      data <- fabric_pbi_dax_query(
        workspace_id = Sys.getenv("FABRIC_WORKSPACE_ID"),
        dataset_id = Sys.getenv("FABRIC_SEMANTIC_MODEL_ID"),
        dax = sprintf(
          r"(
          EVALUATE SUMMARIZECOLUMNS(
            'Product'[Category], TREATAS({%d}, 'Date'[Year]),
            "Sales", [Total Sales]
          )
        )",
          as.integer(year)
        ),
        token = token
      )
      if (ncol(data) == 2L) {
        names(data) <- c("Category", "Sales")
      }
      data
    },
    warehouse = fabric_sql_query(
      server = Sys.getenv("FABRIC_WAREHOUSE_SQL_SERVER"),
      database = Sys.getenv("FABRIC_WAREHOUSE_SQL_DATABASE"),
      sql = "SELECT TOP (100) * FROM dbo.Sales",
      token = token,
      numeric_policy = "driver",
      verbose = FALSE
    ),
    lakehouse = fabric_sql_query(
      server = Sys.getenv("FABRIC_LAKEHOUSE_SQL_SERVER"),
      database = Sys.getenv("FABRIC_LAKEHOUSE_SQL_DATABASE"),
      sql = "SELECT TOP (100) * FROM dbo.Sales",
      token = token,
      numeric_policy = "driver",
      verbose = FALSE
    ),
    kql = fabric_kql_query(
      cluster = Sys.getenv("FABRIC_KQL_ENDPOINT"),
      database = Sys.getenv("FABRIC_KQL_DATABASE"),
      query = "Sales | take 100",
      token = token
    ),
    files = fabric_onelake_read_file(
      workspace = Sys.getenv("FABRIC_WORKSPACE_ID"),
      item = Sys.getenv("FABRIC_LAKEHOUSE_ID"),
      path = Sys.getenv("FABRIC_ONELAKE_FILE", "Files/sales.parquet"),
      token = token
    ),
    delta = fabric_onelake_read_delta_table(
      table_path = Sys.getenv("FABRIC_DELTA_TABLE", "Sales"),
      schema = Sys.getenv("FABRIC_DELTA_SCHEMA", "dbo"),
      workspace_name = Sys.getenv("FABRIC_WORKSPACE_ID"),
      lakehouse_name = Sys.getenv("FABRIC_LAKEHOUSE_ID"),
      limit = 100,
      token = token,
      verbose = FALSE
    ),
    graphql = {
      result <- fabric_graphql_query(
        api = Sys.getenv("FABRIC_GRAPHQL_ENDPOINT"),
        query = "query { sales(first: 100) { items { Category Amount } } }",
        error_policy = "error",
        token = token
      )
      do.call(rbind, lapply(result$data$sales$items, as.data.frame))
    }
  )
}

ui <- fabric_shiny_ui(
  fluidPage(
    titlePanel("Explore Fabric data"),
    actionButton("login", "Sign in"),
    actionButton("logout", "Sign out"),
    selectInput("source", "Data source", sources),
    conditionalPanel(
      "input.source === 'dax'",
      numericInput("year", "Year", 2026, min = 1900, max = 2100, step = 1)
    ),
    bslib::input_task_button("load", "Load data"),
    conditionalPanel("input.source === 'dax'", plotOutput("sales")),
    tableOutput("tables")
  ),
  "fabric",
  config
)

server <- function(input, output, session) {
  fabric <- fabric_shiny_server("fabric", config)
  query <- ExtendedTask$new(function(token, generation, source, year) {
    promises::then(token, function(token) {
      data <- if (use_mirai) {
        mirai::mirai(
          {
            library(fabricQueryR)
            read_data(source, token, year)
          },
          read_data = read_data,
          source = source,
          token = token,
          year = year
        )
      } else {
        promises::future_promise(read_data(source, token, year))
      }
      promises::then(data, function(data) {
        list(generation = generation, source = source, year = year, data = data)
      })
    })
  }) |>
    bslib::bind_task_button("load")

  observeEvent(input$login, fabric$login(), ignoreInit = TRUE)
  observeEvent(input$logout, fabric$logout(), ignoreInit = TRUE)
  observeEvent(
    input$load,
    {
      service <- service_for[[req(input$source)]]
      req(fabric$ready(service))
      query$invoke(
        fabric$access_token(service, async = TRUE),
        fabric$generation(),
        input$source,
        input$year
      )
    },
    ignoreInit = TRUE
  )

  result <- reactive({
    req(fabric$ready(service_for[[req(input$source)]]))
    value <- query$result()
    req(identical(value$generation, fabric$generation()))
    req(identical(value$source, input$source))
    if (input$source == "dax") {
      req(identical(value$year, input$year))
    }
    value$data
  })
  output$tables <- renderTable(head(result(), 100))
  output$sales <- renderPlot({
    req(input$source == "dax")
    data <- result()
    req(nrow(data) > 0)
    barplot(as.numeric(data$Sales), names.arg = data$Category, ylab = "Sales")
  })
}

shinyApp(
  ui,
  server,
  uiPattern = ".*",
  options = list(host = "127.0.0.1", port = 8100)
)
