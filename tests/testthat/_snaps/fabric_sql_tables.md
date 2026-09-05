# SQL table helpers validate before executing queries

    Code
      fabric_sql_read_table("server", "", token = "token")
    Condition
      Error in `.fabric_sql_name()`:
      ! table must be one non-empty SQL identifier

---

    Code
      fabric_sql_read_table("server", "orders", columns = c("id", "id"), token = "token")
    Condition
      Error in `.fabric_sql_projection()`:
      ! columns must be unique

---

    Code
      fabric_sql_read_table("server", "orders", limit = 1.5, token = "token")
    Condition
      Error in `.fabric_sql_limit()`:
      ! limit must be NULL or one non-negative whole number

---

    Code
      fabric_sql_tables("server", sql = "SELECT 1", token = "token")
    Condition
      Error in `.fabric_sql_helper_query()`:
      ! These arguments are controlled by the SQL table helper: sql

