fabric_test_external_shortcut <- function(provider) {
  text <- Sys.getenv("FABRIC_TEST_EXTERNAL_SHORTCUT_MATRIX_JSON")
  if (nzchar(text)) {
    matrix <- jsonlite::fromJSON(text, simplifyVector = FALSE)
    fixture <- matrix[[provider]]
  } else {
    legacy <- Sys.getenv("FABRIC_TEST_EXTERNAL_SHORTCUT_JSON")
    fixture <- if (nzchar(legacy)) {
      jsonlite::fromJSON(legacy, simplifyVector = FALSE)
    } else {
      NULL
    }
    if (!identical(names(fixture$target), provider)) fixture <- NULL
  }
  if (is.null(fixture)) {
    fabric_test_feature_unavailable(
      "external-shortcuts",
      paste("External shortcut fixture missing for provider", provider)
    )
  }
  if (!identical(names(fixture$target), provider)) {
    stop("External shortcut fixture target does not match its provider")
  }
  fixture
}
