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

