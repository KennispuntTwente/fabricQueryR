library(shiny)
library(fabricQueryR)

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
    titlePanel("Your Fabric tables"),
    actionButton("login", "Sign in with Microsoft"),
    actionButton("logout", "Sign out"),
    actionButton("reauthorize", "Sign in again"),
    actionButton("retry", "Retry connection"),
    textOutput("account"),
    textInput("table_name", "Table name contains", ""),
    actionButton("load", "Show tables"),
    tableOutput("tables")
  ),
  id = "fabric",
  config = config
)

server <- function(input, output, session) {
  fabric <- fabric_shiny_server("fabric", config)
  rows <- reactiveVal(NULL)

  observeEvent(input$login, fabric$login(), ignoreInit = TRUE)
  observeEvent(input$logout, fabric$logout(), ignoreInit = TRUE)
  observeEvent(input$reauthorize, fabric$reauthorize(), ignoreInit = TRUE)
  observeEvent(input$retry, fabric$prepare("sql"), ignoreInit = TRUE)
  observeEvent(
    fabric$generation(),
    rows(NULL),
    ignoreNULL = FALSE,
    priority = 50
  )

  output$account <- renderText({
    if (is.null(fabric$generation())) {
      return("Sign in to view your Fabric tables.")
    }
    identity <- fabric$identity()$id_token_claims
    if (!fabric$ready("sql")) {
      return(
        "SQL access is unavailable. Retry the connection or sign in again."
      )
    }
    display_name <- if (is.null(identity$name)) identity$sub else identity$name
    paste("Signed in as", display_name)
  })

  observeEvent(
    input$load,
    {
      req(fabric$ready("sql"))
      generation <- fabric$generation()
      rows(NULL)
      result <- tryCatch(
        fabric_sql_query(
          server = sql_server,
          database = sql_database,
          sql = paste(
            "SELECT TOP (100) TABLE_SCHEMA, TABLE_NAME",
            "FROM INFORMATION_SCHEMA.TABLES",
            "WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE ?",
            "ORDER BY TABLE_SCHEMA, TABLE_NAME"
          ),
          params = list(paste0("%", input$table_name, "%")),
          token = fabric$token_provider(),
          backend = "odbc",
          numeric_policy = "driver",
          idempotent = TRUE,
          verbose = FALSE
        ),
        error = function(error) {
          showNotification(
            "Could not read tables. Check your data access, retry, or sign in again.",
            type = "error"
          )
          NULL
        }
      )
      if (!is.null(result) && identical(generation, fabric$generation())) {
        rows(list(generation = generation, data = result))
      }
    },
    ignoreInit = TRUE
  )

  output$tables <- renderTable({
    req(fabric$ready("sql"))
    value <- req(rows())
    req(identical(value$generation, fabric$generation()))
    value$data
  })
}

shinyApp(ui, server, uiPattern = ".*")
