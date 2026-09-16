# Fabric integration coverage: Java and R Livy batch applications
for (language in c("java", "r")) {
  test_that(paste("Livy batches execute", language, "applications"), {
    if (!fabric_test_feature_required("livy-languages")) {
      skip(
        "Java/R batch execution belongs to the required livy-languages feature lane"
      )
    }
    manifest <- fabric_test_manifest()
    lake <- fabric_test_manifest_item(manifest, "TestLakehouse")
    token <- fabric_test_token_provider()
    auth <- fabric_test_azure_auth_config()
    marker <- kusto_ingestion_source_id()
    folder <- paste0("Files/fabricqueryr-livy-languages-", marker)
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
    source <- if (identical(language, "java")) {
      "livy-batch-java.jar"
    } else {
      "livy-batch-r.R"
    }
    output <- paste0("result-", language)
    fabric_onelake_upload(
      manifest$workspace_id,
      lake$id,
      paste(folder, source, sep = "/"),
      source = testthat::test_path("..", "fixtures", source),
      token = token
    )
    batch <- do.call(
      fabric_livy_batch_submit,
      c(
        list(
          livy_url = lake$livy_url,
          file = paste(uri, source, sep = "/"),
          class_name = if (identical(language, "java")) {
            "FabricQueryRBatchProbe"
          } else {
            NULL
          },
          args = c(paste(uri, output, sep = "/"), marker),
          target_lakehouse_id = lake$id,
          verbose = FALSE
        ),
        auth
      )
    )
    withr::defer(try(batch$cancel(), silent = TRUE))
    batch$wait(timeout = 1200, poll_interval = 5)
    expect_identical(tolower(batch$result(refresh = FALSE)$state), "success")
    path <- paste(folder, output, sep = "/")
    if (identical(language, "r")) {
      files <- fabric_onelake_list(
        manifest$workspace_id,
        lake$id,
        path,
        token = token
      )
      parts <- files$path[grepl("/part-", files$path, fixed = TRUE)]
      expect_length(parts, 1L)
      path <- parts[[1L]]
    }
    actual <- rawToChar(fabric_onelake_download(
      manifest$workspace_id,
      lake$id,
      path,
      token = token
    ))
    expect_identical(trimws(actual), paste0(marker, ":3"))
  })
}
