# Fabric integration coverage: external shortcuts and CSV transformations
test_that("CSV shortcut transforms materialize the expected live Delta rows", {
  manifest <- fabric_test_manifest()
  if (!identical(Sys.getenv("FABRIC_TEST_SHORTCUT_TRANSFORMS"), "true")) {
    fabric_test_feature_unavailable(
      "shortcut-transforms",
      "Set FABRIC_TEST_SHORTCUT_TRANSFORMS=true for a tenant supporting csvToDelta"
    )
  }
  fabric_test_use_delta_runtime()
  token <- fabric_test_token_provider()
  fixture <- fabric_test_manifest_item(manifest, "TestLakehouse")
  item <- fabric_item(manifest$workspace_id, fixture$id, token = token)
  name <- paste0(
    "fabricqueryr_csv_transform_",
    gsub("-", "_", .fabric_lakehouse_staging_id())
  )
  source <- paste0("Files/", name)
  parent <- paste0("Tables/", fixture$schema)
  on.exit(
    {
      try(
        fabric_onelake_shortcut_delete(
          item,
          parent,
          name,
          confirm = TRUE,
          token = token
        ),
        silent = TRUE
      )
      try(
        fabric_onelake_delete(
          manifest$workspace_id,
          item,
          source,
          recursive = TRUE,
          confirm = TRUE,
          token = token
        ),
        silent = TRUE
      )
    },
    add = TRUE
  )
  fabric_onelake_upload(
    manifest$workspace_id,
    item,
    paste0(source, "/part.csv"),
    source = charToRaw(enc2utf8("id;label\n1;caf\u00e9\n2;two\n")),
    token = token
  )
  transform <- list(
    type = "csvToDelta",
    includeSubfolders = FALSE,
    properties = list(
      delimiter = ";",
      useFirstRowAsHeader = TRUE,
      skipFilesWithErrors = FALSE
    )
  )
  operation <- fabric_onelake_shortcuts_bulk_create(
    item,
    shortcuts = list(list(
      path = parent,
      name = name,
      target = item,
      target_path = source,
      transform = transform
    )),
    token = token
  )
  result <- fabric_operation_result(operation, timeout = 300)
  if (!identical(result$value$value[[1L]]$status, "Succeeded")) {
    stop(jsonlite::toJSON(result$value$value[[1L]]$error, auto_unbox = TRUE))
  }
  expect_identical(result$value$value[[1L]]$status, "Succeeded")
  observed <- fabric_onelake_shortcut_get(item, parent, name, token = token)
  expect_identical(observed$raw[[1L]]$transform, transform)
  rows <- fabric_test_eventually(
    function() {
      value <- fabric_lakehouse_read_table(
        item,
        name,
        token = token,
        verbose = FALSE
      )
      if (nrow(value) != 2L) {
        return(NULL)
      }
      value[order(value$id), ]
    },
    attempts = 120L,
    delay = 5
  )
  expect_equal(as.integer(rows$id), 1:2)
  expect_identical(rows$label, c("caf\u00e9", "two"))
})

for (provider in c(
  "adlsGen2",
  "amazonS3",
  "azureBlobStorage",
  "googleCloudStorage",
  "oneDriveSharePoint",
  "s3Compatible",
  "dataverse"
)) {
  local({
    provider <- provider
    test_that(paste("external shortcut validates", provider, "data"), {
      manifest <- fabric_test_manifest()
      fixture <- fabric_test_external_shortcut(provider)
      parent <- fixture$parentPath %||% "Files"
      token <- fabric_test_token_provider()
      lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
      item <- fabric_item(manifest$workspace_id, lakehouse$id, token = token)
      name <- paste0("fabricqueryr_external_", .fabric_lakehouse_staging_id())
      on.exit(
        try(
          fabric_onelake_shortcut_delete(
            item,
            parent,
            name,
            confirm = TRUE,
            token = token
          ),
          silent = TRUE
        ),
        add = TRUE
      )
      fabric_onelake_shortcut_create(
        item,
        parent,
        name,
        target = fixture$target,
        token = token
      )
      observed <- fabric_onelake_shortcut_get(item, parent, name, token = token)
      expect_identical(
        .fabric_shortcut_raw_target(observed$raw[[1L]]$target),
        fixture$target
      )
      if (identical(fixture$validation, "table")) {
        fabric_test_use_delta_runtime()
        rows <- fabric_test_eventually(function() {
          fabric_lakehouse_read_table(
            item,
            name,
            schema = basename(parent),
            columns = unlist(fixture$columns),
            token = token,
            verbose = FALSE
          )
        })
        expected <- jsonlite::fromJSON(jsonlite::toJSON(
          fixture$expectedRows,
          auto_unbox = TRUE
        ))
        expect_equal(as.data.frame(rows), expected, ignore_attr = TRUE)
      } else {
        bytes <- fabric_onelake_download(
          manifest$workspace_id,
          item,
          paste(parent, name, fixture$file, sep = "/"),
          token = token
        )
        expect_identical(bytes, charToRaw(enc2utf8(fixture$expectedText)))
      }
    })
  })
}
