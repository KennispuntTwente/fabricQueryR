# Livy can run SQL, SparkR, PySpark, and Spark code in Microsoft Fabric
# This function is not called automatically because it requires credentials and
# a real Lakehouse Livy endpoint
fabric_livy_query_example <- function() {
  # Find the URL under Lakehouse > Settings > Livy Endpoint
  session_url <- paste0(
    "https://api.fabric.microsoft.com/v1/workspaces/.../",
    "lakehouses/.../livyapi/..."
  )

  sql_result <- fabric_livy_query(
    livy_url = session_url,
    kind = "sql",
    code = "SELECT COUNT(*) AS row_count FROM dbo.example_table"
  )

  sparkr_result <- fabric_livy_query(
    livy_url = session_url,
    kind = "sparkr",
    code = "print(1 + 2)"
  )

  invisible(list(sql = sql_result, sparkr = sparkr_result))
}
