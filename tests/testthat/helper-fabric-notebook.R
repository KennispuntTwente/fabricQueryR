# Create a disposable Spark probe; defer its deletion in the caller's test.
fabric_test_probe_notebook <- function(
  workspace,
  code,
  token,
  envir = parent.frame()
) {
  credential <- fabric_credential(token = token)
  base <- paste0(.fabric_api_base, "/workspaces/", workspace)
  cell <- list(
    cell_type = "code",
    source = code,
    execution_count = NULL,
    outputs = list(),
    metadata = list(
      microsoft = list(language = "python", language_group = "synapse_pyspark")
    )
  )
  content <- jsonlite::toJSON(
    list(
      nbformat = 4L,
      nbformat_minor = 5L,
      metadata = list(
        kernel_info = list(name = "synapse_pyspark"),
        language_info = list(name = "python")
      ),
      cells = list(cell)
    ),
    auto_unbox = TRUE,
    null = "null"
  )
  request <- httr2::request(paste0(base, "/notebooks")) |>
    httr2::req_body_json(
      list(
        displayName = paste0(
          "fabricqueryr_probe_",
          .fabric_lakehouse_staging_id()
        ),
        definition = list(
          format = "ipynb",
          parts = list(list(
            path = "notebook-content.ipynb",
            payloadType = "InlineBase64",
            payload = jsonlite::base64_enc(charToRaw(content))
          ))
        )
      ),
      auto_unbox = TRUE
    )
  operation <- .fabric_operation_submit(
    request,
    credential,
    result_expected = TRUE
  )
  item <- fabric_operation_result(operation, timeout = 600)$value
  withr::defer(
    .httr2_perform(
      httr2::request(paste0(base, "/items/", item$id)) |>
        httr2::req_method("DELETE"),
      credential = credential,
      audience = .fabric_audience$fabric
    ),
    envir = envir
  )
  item$id
}
