# Fabric integration coverage: external shortcuts and CSV transformations
test_that("CSV shortcut transforms materialize the expected live Delta rows", {
  manifest <- fabric_test_manifest()
  skip_if_not(
    identical(Sys.getenv("FABRIC_TEST_SHORTCUT_TRANSFORMS"), "true"),
    "Set FABRIC_TEST_SHORTCUT_TRANSFORMS=true for a tenant supporting the documented csvToDelta REST contract"
  )
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

test_that("external shortcut connections expose independently expected file bytes", {
  manifest <- fabric_test_manifest()
  configuration <- fabric_test_optional_environment(
    "FABRIC_TEST_EXTERNAL_SHORTCUT_JSON",
    "External shortcut connection coverage"
  )
  fixture <- jsonlite::fromJSON(configuration, simplifyVector = FALSE)
  expect_named(
    fixture,
    c("target", "file", "expectedText"),
    ignore.order = TRUE
  )
  expect_false("oneLake" %in% names(fixture$target))
  token <- fabric_test_token_provider()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  item <- fabric_item(manifest$workspace_id, lakehouse$id, token = token)
  name <- paste0("fabricqueryr_external_", .fabric_lakehouse_staging_id())
  on.exit(
    try(
      fabric_onelake_shortcut_delete(
        item,
        "Files",
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
    "Files",
    name,
    target = fixture$target,
    token = token
  )
  observed <- fabric_onelake_shortcut_get(item, "Files", name, token = token)
  expect_identical(
    .fabric_shortcut_raw_target(observed$raw[[1L]]$target),
    fixture$target
  )
  bytes <- fabric_onelake_download(
    manifest$workspace_id,
    item,
    paste("Files", name, fixture$file, sep = "/"),
    token = token
  )
  expect_identical(bytes, charToRaw(enc2utf8(fixture$expectedText)))
})
