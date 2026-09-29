# integer64 bind translation validates parameter counts before sending

    Code
      .fabric_sql_db_get_query(con, "SELECT '?', ?, ?", list(bit64::as.integer64(1)))
    Condition
      Error in `.fabric_sql_parameter_sql()`:
      ! ODBC parameter binding found 2 SQL placeholders for 1 value

---

    Code
      .fabric_sql_db_get_query(con, "SELECT '?'", list(bit64::as.integer64(1)))
    Condition
      Error in `.fabric_sql_parameter_sql()`:
      ! ODBC parameter binding found 0 SQL placeholders for 1 value

# ODBC defaults return driver values and warn once across SQL workflows

    Code
      cat(conditionMessage(warnings[[1L]]), "\n", sep = "")
    Output
      ODBC driver conversion may lose numeric precision
      i Use `backend = "adbc"` for exact conversion, or `numeric_policy = "exact"` to reject unsafe ODBC results
      i Set `numeric_policy = "driver"` to accept driver conversion without this warning
      This warning is displayed once per session.

