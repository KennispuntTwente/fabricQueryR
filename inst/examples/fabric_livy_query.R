# Livy can run SQL, SparkR, PySpark, and Spark code in Microsoft Fabric.
# This function is not called automatically because it requires credentials.
fabric_livy_query_example <- function() {
  # Discover a Lakehouse whose record contains its Fabric Livy endpoint.
  workspace <- fabric_workspaces()[[1L]]
  lakehouse <- fabric_lakehouses(workspace)[[1L]]
  table <- fabric_lakehouse_tables(lakehouse)[1L, ]

  # Build SQL from the discovered table, then close the temporary session.
  sql <- sprintf(
    "SELECT COUNT(*) AS row_count FROM `%s`.`%s`",
    table$schema[[1L]],
    table$name[[1L]]
  )
  sql_result <- fabric_livy_query(
    livy_url = lakehouse,
    kind = "sql",
    code = sql
  )

  # The same discovered Lakehouse can also run SparkR code.
  sparkr_result <- fabric_livy_query(
    livy_url = lakehouse,
    kind = "sparkr",
    code = "print(1 + 2)"
  )

  invisible(list(sql = sql_result, sparkr = sparkr_result))
}
