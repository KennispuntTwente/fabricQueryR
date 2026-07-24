fabric_test_manifest <- function() {
  path <- Sys.getenv(
    "FABRIC_TEST_MANIFEST",
    unset = file.path(getwd(), ".fabric-test-manifest.json")
  )
  testthat::skip_if(
    !file.exists(path),
    paste("Fabric integration manifest not found:", path)
  )
  jsonlite::fromJSON(path, simplifyVector = FALSE)
}

fabric_test_token <- function(variable) {
  token <- Sys.getenv(variable)
  testthat::skip_if(
    !nzchar(token),
    paste("Fabric integration token not set:", variable)
  )
  token
}

fabric_test_spark_table <- function(manifest, lakehouse) {
  paste(
    sprintf(
      "`%s`",
      c(
        manifest$workspace_name,
        lakehouse$display_name,
        lakehouse$schema,
        lakehouse$tables$basic
      )
    ),
    collapse = "."
  )
}

fabric_test_manifest_item <- function(manifest, name) {
  item <- manifest$items[[name]]
    if (is.null(item)) {
      stop(
        sprintf(
          "Fabric integration manifest does not provision required item '%s'",
          name
        ),
        call. = FALSE
      )
  }
  item
}
