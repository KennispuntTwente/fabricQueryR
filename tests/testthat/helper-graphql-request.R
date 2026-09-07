graphql_request_body <- function(request) {
  jsonlite::fromJSON(
    rawToChar(request$body$data),
    simplifyVector = FALSE
  )
}
