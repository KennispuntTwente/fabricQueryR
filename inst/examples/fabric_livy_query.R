# Livy can run SQL, PySpark, Spark, and SparkR code in Microsoft Fabric
# This function is not called automatically because it requires credentials
fabric_livy_query_example <- function() {
  # Discover a Lakehouse whose record contains its Fabric Livy endpoint
  workspace <- fabric_workspaces()[[1L]]
  lakehouse <- fabric_lakehouses(workspace)[[1L]]
  table <- fabric_lakehouse_tables(lakehouse)[1L, ]

  # Build SQL from the discovered table, then close the temporary session
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

  # PySpark avoids the SparkR bridge; R-first code can initialize sparklyr
  # inside a kind = "sparkr" session as described in the Livy vignette
  pyspark_result <- fabric_livy_query(
    livy_url = lakehouse,
    kind = "pyspark",
    code = "print(1 + 2)"
  )

  invisible(list(sql = sql_result, pyspark = pyspark_result))
}
