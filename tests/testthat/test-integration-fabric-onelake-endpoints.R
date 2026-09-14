# Fabric integration coverage: global and regional generic OneLake endpoints

test_that("Delta reads use global and regional generic OneLake endpoints", {
  manifest <- fabric_test_manifest()
  fabric_test_use_delta_runtime()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  for (host in c(
    "api.onelake.fabric.microsoft.com",
    "westeurope-api.onelake.fabric.microsoft.com"
  )) {
    rows <- fabric_onelake_read_delta_table(
      table_path = lakehouse$tables$basic,
      workspace_name = manifest$workspace_id,
      lakehouse_name = lakehouse$id,
      schema = lakehouse$schema,
      columns = c("id", "name"),
      dfs_base = paste0("https://", host),
      token = fabric_test_token_provider(),
      verbose = FALSE
    )
    rows <- rows[order(rows$id), ]
    expect_equal(rows$id, 1:3, info = host)
    expect_identical(rows$name, c("alpha", "beta", "gamma"), info = host)
  }
})
