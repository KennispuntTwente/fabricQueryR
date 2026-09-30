library(shiny)
library(fabricQueryR)

future::plan(future::multisession, workers = 2)

config <- fabric_shiny_config(
  tenant_id = Sys.getenv("ENTRA_TENANT_ID"),
  client_id = Sys.getenv("ENTRA_CLIENT_ID"),
  client_secret = Sys.getenv("ENTRA_CLIENT_SECRET"),
  redirect_uri = Sys.getenv("ENTRA_REDIRECT_URI", "http://localhost:8100/"),
  services = "sql"
)
sql_server <- Sys.getenv("FABRIC_SQL_SERVER")
sql_database <- Sys.getenv("FABRIC_SQL_DATABASE")

ui <- fabric_shiny_ui(
  fluidPage(
    actionButton("login", "Sign in"),
    actionButton("logout", "Sign out"),
    textInput("table_name", "Table name contains", ""),
    bslib::input_task_button("load", "Show tables"),
    tableOutput("tables")
  ),
  "fabric",
  config
)

server <- function(input, output, session) {
  fabric <- fabric_shiny_server("fabric", config)
  query <- ExtendedTask$new(function(token, generation, table_name) {
    promises::then(token, function(token) {
      promises::future_promise({
        data <- fabric_sql_query(
          sql_server,
          paste(
            "SELECT TOP (100) TABLE_SCHEMA, TABLE_NAME",
            "FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE ?",
            "ORDER BY TABLE_SCHEMA, TABLE_NAME"
          ),
          database = sql_database,
          params = list(paste0("%", table_name, "%")),
          token = token,
          numeric_policy = "driver",
          verbose = FALSE
        )
        list(generation = generation, data = data)
      })
    })
  }) |>
    bslib::bind_task_button("load")

  observeEvent(input$login, fabric$login(), ignoreInit = TRUE)
  observeEvent(input$logout, fabric$logout(), ignoreInit = TRUE)
  observeEvent(
    input$load,
    {
      req(fabric$ready("sql"))
      query$invoke(
        fabric$access_token("sql", async = TRUE),
        fabric$generation(),
        input$table_name
      )
    },
    ignoreInit = TRUE
  )

  output$tables <- renderTable({
    req(fabric$ready("sql"))
    result <- query$result()
    req(identical(result$generation, fabric$generation()))
    result$data
  })
}

shinyApp(
  ui,
  server,
  uiPattern = ".*",
  options = list(host = "127.0.0.1", port = 8100)
)
