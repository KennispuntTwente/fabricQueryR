# Fabric integration coverage: case-sensitive bulk shortcut destinations

test_that("bulk shortcuts preserve case-distinct parent paths in OneLake", {
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  item <- fabric_item(manifest$workspace_id, lakehouse$id, token = token)
  root <- paste0("Files/fabricqueryr_case_", .fabric_lakehouse_staging_id())
  parents <- paste0(root, c("/A", "/a"))
  on.exit(
    {
      for (parent in parents) {
        try(
          fabric_onelake_shortcut_delete(
            item,
            parent,
            "orders",
            confirm = TRUE,
            token = token
          ),
          silent = TRUE
        )
      }
      try(
        fabric_onelake_delete(
          manifest$workspace_id,
          item,
          root,
          recursive = TRUE,
          confirm = TRUE,
          token = token
        ),
        silent = TRUE
      )
    },
    add = TRUE
  )
  for (parent in parents) {
    fabric_onelake_upload(
      manifest$workspace_id,
      item,
      paste0(parent, "/marker.txt"),
      source = charToRaw(parent),
      token = token
    )
  }

  operation <- fabric_onelake_shortcuts_bulk_create(
    item,
    lapply(parents, function(parent) {
      list(
        path = parent,
        name = "orders",
        target = item,
        target_path = "Files/fixtures/nested"
      )
    }),
    token = token
  )
  responses <- fabric_operation_result(operation, timeout = 300)$value$value
  expect_identical(
    vapply(responses, `[[`, character(1), "status"),
    rep("Succeeded", 2L)
  )
  for (parent in parents) {
    shortcut <- fabric_onelake_shortcut_get(
      item,
      parent,
      "orders",
      token = token
    )
    expect_identical(shortcut$path, parent)
    expect_identical(shortcut$name, "orders")
  }
})
