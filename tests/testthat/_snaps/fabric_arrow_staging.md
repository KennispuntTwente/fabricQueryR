# Parquet staging preserves existing files when a part collides

    Code
      .fabric_parquet_write_dataset(prepared, directory, "snappy", target_file_size = 1024^
        2, max_rows_per_file = 1, caller = "test", error_class = "fabric_arrow_error")
    Condition
      Error in `.fabric_parquet_write_dataset()`:
      ! Could not serialize data to partitioned Parquet for test
      Caused by error in `open_file()`:
      ! Parquet staging would overwrite an existing local file

---

    Code
      .fabric_parquet_write_dataset(prepared, directory, "snappy", target_file_size = 1024^
        2, max_rows_per_file = 1, caller = "test", error_class = "fabric_arrow_error")
    Condition
      Error in `.fabric_parquet_write_dataset()`:
      ! Could not serialize data to partitioned Parquet for test
      Caused by error in `open_file()`:
      ! Parquet staging would overwrite an existing local file

