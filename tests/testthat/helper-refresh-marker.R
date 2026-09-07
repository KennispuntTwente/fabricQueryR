# A disposable Import model whose source marker changes with the source clock.
fabric_test_marker_model <- function(
  workspace_id,
  token,
  .local_envir = parent.frame()
) {
  name <- paste0(
    "fabricqueryr_refresh_marker_",
    basename(tempfile()),
    "_",
    Sys.getpid()
  )
  credential <- fabric_credential(token = token)
  base <- paste0(.fabric_api_base, "/workspaces/", workspace_id)
  withr::defer(
    {
      items <- fabric_items(workspace_id, type = "SemanticModel", token = token)
      matches <- Filter(function(item) identical(item$displayName, name), items)
      for (item in matches) {
        .httr2_perform(
          httr2::request(paste0(base, "/items/", item$id)) |>
            httr2::req_method("DELETE"),
          credential = credential,
          audience = .fabric_audience$fabric
        )
      }
    },
    envir = .local_envir
  )
  definition <- list(
    compatibilityLevel = 1600L,
    model = list(
      culture = "en-US",
      defaultPowerBIDataSourceVersion = "powerBI_V3",
      tables = list(list(
        name = "Marker",
        columns = list(list(
          name = "marker",
          dataType = "string",
          sourceColumn = "marker"
        )),
        partitions = list(list(
          name = "Marker",
          mode = "import",
          source = list(
            type = "m",
            expression = '#table(type table [marker = text], {{DateTimeZone.ToText(DateTimeZone.UtcNow())}})'
          )
        ))
      ))
    )
  )
  part <- function(path, value) {
    list(
      path = path,
      payloadType = "InlineBase64",
      payload = jsonlite::base64_enc(charToRaw(jsonlite::toJSON(
        value,
        auto_unbox = TRUE
      )))
    )
  }
  payload <- list(
    displayName = name,
    definition = list(
      parts = list(
        part("model.bim", definition),
        part(
          "definition.pbism",
          list(version = "5.0", settings = list(qnaEnabled = FALSE))
        )
      )
    )
  )
  response <- .httr2_perform(
    httr2::request(paste0(base, "/semanticModels")) |>
      httr2::req_body_json(payload, auto_unbox = TRUE),
    credential = credential,
    audience = .fabric_audience$fabric,
    idempotent = FALSE
  )
  if (httr2::resp_status(response) == 202L) {
    fabric_operation_wait(
      httr2::resp_header(response, "x-ms-operation-id"),
      token = token,
      timeout = 600
    )
  }
  items <- fabric_items(workspace_id, type = "SemanticModel", token = token)
  Filter(function(item) identical(item$displayName, name), items)[[1L]]
}
