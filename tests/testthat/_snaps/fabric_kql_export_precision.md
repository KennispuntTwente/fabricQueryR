# Parquet export refuses decimal schemas before any write

    Code
      export()
    Condition
      Error in `kusto_export_validate_decimal_schema()`:
      ! KQL Parquet export can change decimal values in "value1"
      i No export was submitted
      i Project decimal values as strings with a companion isnull() flag, or explicitly use `numeric_policy = "service"`

