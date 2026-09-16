# Fabric integration coverage: Livy batch dependencies and environments
test_that("Livy batches execute with published environments and file dependencies", {
  manifest <- fabric_test_manifest()
  lake <- fabric_test_manifest_item(manifest, "TestLakehouse")
  environment <- fabric_test_manifest_item(manifest, "TestEnvironment")
  token <- fabric_test_token_provider()
  marker <- kusto_ingestion_source_id()
  folder <- paste0("Files/fabricqueryr-livy-dependencies-", marker)
  uri <- paste0(
    "abfss://",
    manifest$workspace_id,
    "@onelake.dfs.fabric.microsoft.com/",
    lake$id,
    "/",
    folder
  )
  withr::defer(try(
    fabric_onelake_delete(
      manifest$workspace_id,
      lake$id,
      folder,
      recursive = TRUE,
      confirm = TRUE,
      token = token
    ),
    silent = TRUE
  ))
  sources <- c(
    "livy-batch-dependencies.py",
    "job-dependency.py",
    "job-resource.jar",
    "job-archive.zip"
  )
  remote <- c(
    "main.py",
    "fabricqueryr_dependency.py",
    "resource.jar",
    "archive.zip"
  )
  for (i in seq_along(sources)) {
    fabric_onelake_upload(
      manifest$workspace_id,
      lake$id,
      paste(folder, remote[[i]], sep = "/"),
      source = testthat::test_path("..", "fixtures", sources[[i]]),
      token = token
    )
  }
  fabric_onelake_upload(
    manifest$workspace_id,
    lake$id,
    paste0(folder, "/plain.txt"),
    source = charToRaw("fabricqueryr-dependency-73"),
    token = token
  )
  batch <- do.call(
    fabric_livy_batch_submit,
    c(
      list(
        livy_url = lake$livy_url,
        file = paste0(uri, "/main.py"),
        args = c(paste0(uri, "/result.json"), marker),
        jars = paste0(uri, "/resource.jar"),
        files = paste0(uri, "/plain.txt"),
        py_files = paste0(uri, "/fabricqueryr_dependency.py"),
        archives = paste0(uri, "/archive.zip"),
        environment_id = environment$id,
        conf = list("spark.sql.shuffle.partitions" = "3"),
        target_lakehouse_id = lake$id,
        verbose = FALSE
      ),
      fabric_test_azure_auth_config()
    )
  )
  withr::defer(try(batch$cancel(), silent = TRUE))
  batch$wait(timeout = 1200, poll_interval = 5, error_on_failure = FALSE)
  result <- tryCatch(
    jsonlite::fromJSON(rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lake$id,
      paste0(folder, "/result.json"),
      token = token
    ))),
    fabric_http_error = function(error) NULL
  )
  expect_identical(tolower(batch$state), "success", info = result$error)
  expect_type(result, "list")
  expect_identical(result$marker, marker)
  expect_identical(result$dependencies, "fabricqueryr-dependency-73")
})
