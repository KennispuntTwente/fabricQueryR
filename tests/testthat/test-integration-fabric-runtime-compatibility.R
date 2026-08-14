# Fabric integration coverage: cross-runtime compatibility smoke tests
# These complement the full OneLake tests on the
# preview runtime and the full Livy tests on the core runtime

test_that("delta-rs reads a Fabric table on the core Spark runtime", {
  skip_if_not(identical(Sys.getenv("FABRIC_SPARK_RUNTIME_LANE"), "core"))
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")

  result <- fabric_onelake_read_delta_table(
    table_path = lakehouse$tables$basic,
    workspace_name = manifest$workspace_id,
    lakehouse_name = lakehouse$id,
    schema = lakehouse$schema,
    token = fabric_test_token_provider(),
    verbose = FALSE
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(sort(result$id), 1:3)
})

test_that("Livy executes PySpark on the preview Spark runtime", {
  skip_if_not(identical(Sys.getenv("FABRIC_SPARK_RUNTIME_LANE"), "preview"))
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  auth <- fabric_test_azure_auth_config()

  result <- fabric_livy_query(
    livy_url = lakehouse$livy_url,
    code = 'print("FABRICQUERYR_PREVIEW_LIVY_OK")',
    kind = "pyspark",
    tenant_id = auth$tenant_id,
    client_id = auth$client_id,
    auth_args = auth$auth_args,
    verbose = FALSE
  )

  expect_equal(result$state, "available")
  expect_equal(result$output$status, "ok")
  expect_match(
    paste(result$output$parsed, collapse = "\n"),
    "FABRICQUERYR_PREVIEW_LIVY_OK",
    fixed = TRUE
  )
})
