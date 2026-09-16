# Fabric integration coverage: kql graphql datetime
test_that("KQL Parquet datetime precision is explicit and full precision can use text", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("arrow")
  token <- fabric_test_token_provider()
  database <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestKQLDatabase")$id,
    token = token
  )
  lake <- fabric_test_manifest_item(manifest, "TestLakehouse")
  root <- paste0("Files/fabricqueryr-datetime-", kusto_ingestion_source_id())
  withr::defer(try(
    fabric_onelake_delete(
      manifest$workspace_id,
      lake$id,
      root,
      recursive = TRUE,
      confirm = TRUE,
      token = token
    ),
    silent = TRUE
  ))
  query <- paste(
    "datatable(id:int, moment:datetime) [",
    "1, datetime(2026-09-16T01:02:03.1230000Z),",
    "2, datetime(2026-09-16T01:02:03.1234560Z),",
    "3, datetime(2026-09-16T01:02:03.1234567Z),",
    "4, datetime(null)]",
    "| extend precise = format_datetime(moment, 'yyyy-MM-dd HH:mm:ss.fffffff')"
  )
  for (precision in list(NULL, "millisecond", "microsecond")) {
    path <- paste0(root, "/", precision %||% "default")
    exported <- fabric_kql_export(
      database,
      query,
      destination = paste0(
        "https://onelake.dfs.fabric.microsoft.com/",
        manifest$workspace_id,
        "/",
        lake$id,
        "/",
        path
      ),
      parquet_datetime_precision = precision,
      token = token,
      timeout = 300
    )
    expect_identical(exported$state, "Completed")
    files <- fabric_onelake_list(
      manifest$workspace_id,
      lake$id,
      path,
      recursive = TRUE,
      token = token
    )
    files <- files$path[!files$is_directory & grepl("[.]parquet$", files$path)]
    expect_gt(length(files), 0L)
    columns <- lapply(files, function(file) {
      local <- tempfile(fileext = ".parquet")
      withr::defer(unlink(local))
      fabric_onelake_download(
        manifest$workspace_id,
        lake$id,
        file,
        dest = local,
        token = token
      )
      table <- arrow::read_parquet(local, as_data_frame = FALSE)
      # Compare integer microseconds; POSIXct doubles cannot represent 100 ns.
      data.frame(
        id = as.vector(table$id),
        ticks = as.character(table$moment$cast(arrow::timestamp(
          "us"
        ))$cast(arrow::int64())),
        precise = as.vector(table$precise)
      )
    })
    rows <- do.call(rbind, columns)
    rows <- rows[order(rows$id), ]
    expect_identical(rows$id, 1:4)
    expect_identical(
      rows$ticks,
      if (identical(precision, "microsecond")) {
        c(
          "1789520523123000",
          "1789520523123456",
          "1789520523123456",
          NA_character_
        )
      } else {
        c(rep("1789520523123000", 3L), NA_character_)
      }
    )
    expect_identical(
      rows$precise,
      c(
        "2026-09-16 01:02:03.1230000",
        "2026-09-16 01:02:03.1234560",
        "2026-09-16 01:02:03.1234567",
        ""
      )
    )
  }
})
