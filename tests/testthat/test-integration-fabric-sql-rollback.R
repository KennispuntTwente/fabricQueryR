test_that("Warehouse overwrite rolls back destructive SQL when loading fails", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  warehouse_fixture <- fabric_test_manifest_item(manifest, "TestWarehouse")
  lakehouse_fixture <- fabric_test_manifest_item(manifest, "TestLakehouse")
  warehouse <- fabric_item(
    manifest$workspace_id,
    warehouse_fixture$id,
    type = "Warehouse",
    token = token
  )
  staging_lakehouse <- fabric_item(
    manifest$workspace_id,
    lakehouse_fixture$id,
    type = "Lakehouse",
    token = token
  )
  execute <- .fabric_warehouse_execute

  for (backend in fabric_test_sql_backends()) {
    for (method in c("Truncate", "Drop")) {
      local({
        table <- paste0("rollback_", gsub("-", "", kusto_ingestion_source_id()))
        table_sql <- paste0("[dbo].[", table, "]")
        con <- fabric_sql_connect(
          warehouse,
          backend = backend,
          token = token,
          read_only = FALSE,
          verbose = FALSE
        )
        on.exit(.fabric_warehouse_disconnect(con, backend), add = TRUE)
        on.exit(
          DBI::dbExecute(con, paste("DROP TABLE IF EXISTS", table_sql)),
          add = TRUE,
          after = FALSE
        )
        DBI::dbExecute(
          con,
          paste(
            "CREATE TABLE",
            table_sql,
            "([id] int NOT NULL, [label] varchar(30) NOT NULL)"
          )
        )
        DBI::dbExecute(
          con,
          paste(
            "INSERT INTO",
            table_sql,
            "VALUES (1, 'original-a'), (2, 'original-b')"
          )
        )
        original <- DBI::dbGetQuery(
          con,
          paste("SELECT id, label FROM", table_sql, "ORDER BY id")
        )
        destructive_completed <- FALSE
        load_attempted <- FALSE
        local_mocked_bindings(.fabric_warehouse_execute = function(
          connection,
          sql
        ) {
          if (grepl("^(TRUNCATE|DROP) TABLE", sql)) {
            result <- execute(connection, sql)
            destructive_completed <<- TRUE
            return(result)
          }
          expect_identical(destructive_completed, TRUE)
          # The real COPY/CTAS reaches Fabric with a nonexistent Parquet source.
          sql <- sub("/*.parquet", "/missing/part.parquet", sql, fixed = TRUE)
          load_attempted <<- TRUE
          execute(connection, sql)
        })
        error <- rlang::catch_cnd(
          fabric_warehouse_write_table(
            warehouse,
            table,
            data.frame(id = 3L, label = "replacement"),
            staging_lakehouse = staging_lakehouse,
            mode = "Overwrite",
            overwrite_method = method,
            keep_staging_on_failure = FALSE,
            backend = backend,
            token = token,
            verbose = FALSE
          ),
          classes = "error"
        )
        expect_identical(destructive_completed, TRUE)
        expect_identical(load_attempted, TRUE)
        expect_s3_class(error, "fabric_warehouse_write_error")
        if (isTRUE(error$staging_retained)) {
          on.exit(
            fabric_onelake_delete(
              manifest$workspace_id,
              staging_lakehouse,
              error$staging_path,
              recursive = TRUE,
              confirm = TRUE,
              token = token
            ),
            add = TRUE,
            after = FALSE
          )
        }
        restored <- DBI::dbGetQuery(
          con,
          paste("SELECT id, label FROM", table_sql, "ORDER BY id")
        )
        expect_identical(restored, original, info = paste(backend, method))
      })
    }
  }
})
