playground_shiny_mode <- function(
  mode = Sys.getenv("FABRIC_SHINY_AUTH", "auto")
) {
  mode <- match.arg(mode, c("auto", "sandbox", "delegated"))
  configured <- nzchar(Sys.getenv(c(
    "ENTRA_TENANT_ID",
    "ENTRA_CLIENT_ID",
    "ENTRA_CLIENT_SECRET"
  )))
  if (mode == "auto") {
    mode <- if (any(configured)) "delegated" else "sandbox"
  }
  if (mode == "delegated" && !all(configured)) {
    cli::cli_abort(
      "Delegated mode needs ENTRA_TENANT_ID, ENTRA_CLIENT_ID and ENTRA_CLIENT_SECRET."
    )
  }
  mode
}

playground_shiny_dispatcher <- function(repository) {
  if (requireNamespace("mirai", quietly = TRUE)) {
    pool <- "fabricqueryr-playground"
    mirai::daemons(2, .compute = pool)
    shiny::onStop(function() mirai::daemons(0, .compute = pool), session = NULL)
    dispatch <- function(requests, tokens) {
      mirai::mirai(
        {
          devtools::load_all(repository, quiet = TRUE)
          source(
            file.path(repository, "playground", "shiny", "queries.R"),
            local = TRUE
          )
          playground_shiny_run(requests, tokens)
        },
        repository = repository,
        requests = requests,
        tokens = tokens,
        .compute = pool
      )
    }
    attr(dispatch, "backend") <- "mirai"
  } else {
    previous <- future::plan()
    future::plan(future::multisession, workers = 2)
    shiny::onStop(function() future::plan(previous), session = NULL)
    dispatch <- function(requests, tokens) {
      promises::future_promise(
        {
          devtools::load_all(repository, quiet = TRUE)
          source(
            file.path(repository, "playground", "shiny", "queries.R"),
            local = TRUE
          )
          playground_shiny_run(requests, tokens)
        },
        globals = list(
          repository = repository,
          requests = requests,
          tokens = tokens
        )
      )
    }
    attr(dispatch, "backend") <- "future"
  }
  dispatch
}

playground_shiny_app <- function(
  sandbox,
  repository,
  mode = playground_shiny_mode(),
  dispatch = playground_shiny_dispatcher(repository),
  config = NULL
) {
  # as_list() drops the R6 object's retained credential and methods.
  record <- function(item) {
    if (is.null(item)) {
      NULL
    } else if (is.environment(item)) {
      item$as_list()
    } else {
      item
    }
  }
  workspace <- record(sandbox$workspace)
  targets <- lapply(sandbox$targets, record)
  sources <- playground_shiny_sources()
  sources$available <- vapply(
    sources$target,
    function(key) {
      is.na(key) || !is.null(targets[[key]])
    },
    logical(1)
  )
  if (mode == "delegated" && is.null(config)) {
    config <- fabricQueryR::fabric_shiny_config(
      tenant_id = Sys.getenv("ENTRA_TENANT_ID"),
      client_id = Sys.getenv("ENTRA_CLIENT_ID"),
      client_secret = Sys.getenv("ENTRA_CLIENT_SECRET"),
      redirect_uri = Sys.getenv("ENTRA_REDIRECT_URI", "http://localhost:8100/"),
      services = unique(sources$service)
    )
  }
  choices <- stats::setNames(
    sources$source,
    paste0(sources$label, ifelse(sources$available, "", " (fixture missing)"))
  )
  ui <- bslib::page_sidebar(
    title = "Fabric + Shiny playground",
    theme = bslib::bs_theme(version = 5, bootswatch = "flatly"),
    sidebar = bslib::sidebar(
      width = 340,
      shiny::p(workspace$displayName),
      shiny::uiOutput("identity"),
      if (mode == "delegated") {
        shiny::tagList(
          shiny::actionButton("login", "Sign in"),
          shiny::actionButton("logout", "Sign out"),
          shiny::actionButton("prepare", "Retry service tokens"),
          shiny::actionButton("reauthorize", "Reauthorize")
        )
      },
      shiny::hr(),
      shiny::selectInput(
        "source",
        "Data source",
        choices,
        selected = "warehouse"
      ),
      shiny::conditionalPanel(
        "['warehouse','lakehouse','snapshot','sql_database'].includes(input.source)",
        shiny::selectInput(
          "backend",
          "SQL driver",
          c("ODBC" = "odbc", "ADBC" = "adbc")
        ),
        shiny::numericInput(
          "min_id",
          "Minimum ID",
          1,
          min = 1,
          max = 3,
          step = 1
        )
      ),
      shiny::conditionalPanel(
        "['dax','dax_arrow','kql'].includes(input.source)",
        shiny::selectInput("category", "Category", c("All", "A", "B"))
      ),
      bslib::input_task_button("load", "Run selected source"),
      shiny::actionButton("load_all", "Run available sources"),
      shiny::hr(),
      shiny::actionButton("ping", "Check responsiveness"),
      shiny::textOutput("ping_status"),
      shiny::textOutput("task_status"),
      shiny::p(paste("Background queries:", attr(dispatch, "backend")))
    ),
    bslib::navset_card_tab(
      id = "view",
      bslib::nav_panel(
        "Results",
        shiny::uiOutput("source_note"),
        shiny::textOutput("result_status"),
        shiny::conditionalPanel(
          "['dax','dax_arrow'].includes(input.source)",
          shiny::plotOutput("amounts", height = "250px")
        ),
        shiny::tableOutput("data")
      ),
      bslib::nav_panel(
        "Query",
        shiny::p("The query below uses the selected controls."),
        shiny::verbatimTextOutput("query_text")
      ),
      bslib::nav_panel("Run results", shiny::tableOutput("run_results")),
      bslib::nav_panel(
        "Connection",
        shiny::tableOutput("services"),
        shiny::p(
          "A ready token does not prove access to a Fabric item. Run a source to check its data access."
        ),
        shiny::tableOutput("fixtures")
      )
    )
  )
  if (mode == "delegated") {
    ui <- fabricQueryR::fabric_shiny_ui(ui, "fabric", config)
  }

  server <- function(input, output, session) {
    auth <- if (mode == "delegated") {
      fabricQueryR::fabric_shiny_server("fabric", config)
    } else {
      NULL
    }
    generation <- function() if (is.null(auth)) "sandbox" else auth$generation()
    ready <- function(service) if (is.null(auth)) TRUE else auth$ready(service)
    request_for <- function(source) {
      playground_shiny_request(
        source,
        workspace,
        targets,
        mode,
        category = input$category %||% "All",
        min_id = input$min_id %||% 1,
        backend = input$backend %||% "odbc"
      )
    }
    token_for <- function(service) {
      # Acquire delegated tokens in the owning session before handing them to a
      # worker. Catch each failure so one unavailable service does not hide others.
      result <- tryCatch(
        {
          if (is.null(auth)) {
            promises::promise_resolve(sandbox$token(playground_shiny_audience(
              service,
              mode
            )))
          } else {
            auth$access_token(service, async = TRUE)
          }
        },
        error = function(error) promises::promise_resolve(error)
      )
      promises::then(result, onRejected = function(error) error)
    }
    query <- shiny::ExtendedTask$new(function(
      requests,
      token_promises,
      generation
    ) {
      promises::then(
        promises::promise_all(.list = token_promises),
        function(tokens) {
          promises::then(dispatch(requests, tokens), function(results) {
            list(generation = generation, results = results)
          })
        }
      )
    }) |>
      bslib::bind_task_button("load")

    run <- function(selected) {
      if (identical(query$status(), "running")) {
        shiny::showNotification("A query is already running.")
        return(invisible(NULL))
      }
      if (is.null(generation())) {
        shiny::showNotification(
          "Sign in before querying Fabric.",
          type = "message"
        )
        return(invisible(NULL))
      }
      requests <- tryCatch(
        stats::setNames(lapply(selected, request_for), selected),
        error = function(error) {
          shiny::showNotification(conditionMessage(error), type = "error")
          NULL
        }
      )
      if (is.null(requests)) {
        return(invisible(NULL))
      }
      services <- unique(vapply(requests, `[[`, character(1), "service"))
      query$invoke(
        requests,
        stats::setNames(lapply(services, token_for), services),
        generation()
      )
    }
    shiny::observeEvent(input$load, run(input$source), ignoreInit = TRUE)
    shiny::observeEvent(
      input$load_all,
      run(sources$source[sources$available]),
      ignoreInit = TRUE
    )
    if (!is.null(auth)) {
      shiny::observeEvent(input$login, auth$login(), ignoreInit = TRUE)
      shiny::observeEvent(input$logout, auth$logout(), ignoreInit = TRUE)
      shiny::observeEvent(input$prepare, auth$prepare(), ignoreInit = TRUE)
      shiny::observeEvent(
        input$reauthorize,
        auth$reauthorize(),
        ignoreInit = TRUE
      )
    }
    current_results <- shiny::reactive({
      shiny::req(!is.null(generation()))
      value <- query$result()
      shiny::req(identical(value$generation, generation()))
      value$results
    })
    selected_result <- shiny::reactive({
      value <- current_results()[[shiny::req(input$source)]]
      shiny::req(!is.null(value))
      shiny::req(identical(value$request, request_for(input$source)))
      # Errors remain visible even when service-token preparation failed.
      if (value$ok) {
        shiny::req(ready(value$request$service))
      }
      value
    })
    output$data <- shiny::renderTable(
      {
        value <- selected_result()
        shiny::validate(shiny::need(value$ok, value$error))
        head(value$data, 100)
      },
      striped = TRUE,
      bordered = FALSE
    )
    output$result_status <- shiny::renderText({
      value <- selected_result()
      if (!value$ok) {
        return(paste("Query failed:", value$error))
      }
      sprintf(
        "%d rows / %.2f seconds / worker process %d%s",
        nrow(value$data),
        value$seconds,
        value$pid,
        if (is.null(attr(value$data, "pages"))) {
          ""
        } else {
          paste0(" / ", attr(value$data, "pages"), " GraphQL pages")
        }
      )
    })
    output$amounts <- shiny::renderPlot({
      shiny::req(input$source %in% c("dax", "dax_arrow"))
      value <- selected_result()
      shiny::req(value$ok, nrow(value$data) > 0)
      graphics::barplot(
        as.numeric(value$data$Amount),
        names.arg = value$data$Category,
        ylab = "Amount",
        col = "#168a80",
        border = NA
      )
    })
    output$query_text <- shiny::renderText({
      request <- tryCatch(
        request_for(shiny::req(input$source)),
        error = function(error) error
      )
      if (inherits(request, "error")) {
        return(conditionMessage(request))
      }
      paste(
        request$statement,
        if (!is.null(request$min_id)) {
          paste0("\n# Bound parameter: min_id = ", request$min_id)
        },
        if (request$source == "kql") {
          paste0("\n# Bound parameter: selected_category = ", request$category)
        },
        collapse = "\n"
      )
    })
    output$run_results <- shiny::renderTable({
      results <- current_results()
      do.call(
        rbind,
        lapply(results, function(value) {
          data.frame(
            Source = value$request$label,
            Status = if (value$ok) "OK" else "Failed",
            Rows = if (value$ok) nrow(value$data) else NA_integer_,
            Seconds = round(value$seconds, 2),
            Message = value$error %||% ""
          )
        })
      )
    })
    output$source_note <- shiny::renderUI({
      row <- sources[sources$source == shiny::req(input$source), ]
      if (!row$available) {
        return(shiny::p(
          "This fixture is missing from the sandbox. Restore it and restart the app to rediscover the targets."
        ))
      }
      if (row$service == "dax") {
        shiny::p(
          "DAX calculates totals in the semantic model. The seeded categories total A = 10.5 and B = 20; use the filter to update the chart."
        )
      } else if (row$service != "fabric") {
        shiny::p(
          "The fixture contains alpha, beta and gamma (IDs 1-3). Their amounts total 30.5. Run the query to read the actual Fabric data."
        )
      } else {
        shiny::p("Lists the items visible to the current Fabric identity.")
      }
    })
    output$identity <- shiny::renderUI({
      if (is.null(auth)) {
        return(shiny::tagList(
          shiny::h5("Shared sandbox connection"),
          shiny::p(
            if (sandbox$principal_type == "service_principal") {
              "Queries use the sandbox application identity."
            } else {
              "Queries use the R session's sandbox identity."
            }
          ),
          shiny::p(
            "Use delegated mode to try each visitor's own Fabric access."
          )
        ))
      }
      if (is.null(generation())) {
        return(shiny::p("Delegated access / signed out"))
      }
      identity <- auth$identity()
      shiny::tagList(
        shiny::h5("Delegated access"),
        shiny::p(
          identity$preferred_username %||% identity$name %||% "Signed in"
        )
      )
    })
    output$services <- shiny::renderTable({
      if (is.null(auth)) {
        return(data.frame(
          Service = unique(sources$service),
          Authentication = "Shared sandbox connection"
        ))
      }
      status <- auth$status()$services
      if (!length(status)) {
        return(data.frame(Status = "Sign in to prepare service tokens"))
      }
      do.call(
        rbind,
        lapply(names(status), function(service) {
          data.frame(
            Service = service,
            Status = status[[service]]$status,
            Ready = status[[service]]$ready,
            Reason = status[[service]]$reason %||% ""
          )
        })
      )
    })
    output$fixtures <- shiny::renderTable(data.frame(
      Source = sources$label,
      Fixture = ifelse(sources$available, "Discovered", "Missing")
    ))
    output$task_status <- shiny::renderText(paste("Task:", query$status()))
    output$ping_status <- shiny::renderText(paste(
      "Responses:",
      input$ping %||% 0L
    ))
  }
  shiny::shinyApp(
    ui,
    server,
    uiPattern = ".*",
    options = list(host = "127.0.0.1", port = 8100)
  )
}

`%||%` <- function(x, y) if (is.null(x)) y else x
