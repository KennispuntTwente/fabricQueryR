kql_export_schema_response <- function(
  types = "string",
  second = NULL,
  alter = identity
) {
  make_schema <- function(types, id) {
    storage_types <- c(
      bool = "System.SByte",
      datetime = "System.DateTime",
      decimal = "System.Data.SqlTypes.SqlDecimal",
      dynamic = "System.Object",
      guid = "System.Guid",
      int = "System.Int32",
      long = "System.Int64",
      real = "System.Double",
      string = "System.String",
      timespan = "System.TimeSpan"
    )
    list(
      FrameType = "DataTable",
      TableId = id,
      TableKind = "PrimaryResult",
      TableName = paste0("Schema", id),
      Columns = list(
        list(ColumnName = "ColumnName", ColumnType = "string"),
        list(ColumnName = "ColumnOrdinal", ColumnType = "int"),
        list(ColumnName = "DataType", ColumnType = "string"),
        list(ColumnName = "ColumnType", ColumnType = "string")
      ),
      Rows = lapply(seq_along(types), function(index) {
        list(
          paste0("value", index),
          index - 1L,
          unname(storage_types[types[[index]]]) %||% "System.String",
          types[[index]]
        )
      })
    )
  }
  frames <- list(
    list(FrameType = "DataSetHeader", Version = "v2.0", IsProgressive = FALSE),
    make_schema(types, 0L)
  )
  if (!is.null(second)) {
    frames <- c(frames, list(make_schema(second, 1L)))
  }
  kusto_test_response(alter(c(frames, list(kusto_test_completion()))))
}
