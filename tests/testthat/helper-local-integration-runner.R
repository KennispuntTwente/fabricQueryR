fabric_test_local_runner <- function() {
  root <- tryCatch(
    fabric_test_repository_root(),
    error = function(error) {
      skip("Local integration runner tests require a repository checkout")
    }
  )
  runner <- file.path(
    root,
    "tools",
    "fabric-sandbox",
    "local-integration.R"
  )
  if (!file.exists(runner)) {
    skip("Local integration runner tests require a repository checkout")
  }
  environment <- new.env(parent = globalenv())
  sys.source(runner, envir = environment)
  environment
}

fabric_test_local_jwt <- function(claims) {
  payload <- jsonlite::toJSON(claims, auto_unbox = TRUE)
  payload <- jsonlite::base64_enc(charToRaw(payload))
  payload <- gsub("[[:space:]=]", "", payload)
  payload <- chartr("+/", "-_", payload)
  paste("header", payload, "signature", sep = ".")
}
