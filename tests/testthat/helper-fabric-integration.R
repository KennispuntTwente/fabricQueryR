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
