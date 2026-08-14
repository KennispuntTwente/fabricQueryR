# Prepare an R or Arrow object for one-pass Parquet serialization. Returns a
# RecordBatchReader so lazy Arrow inputs are never collected into an R object.
.fabric_parquet_prepare_data <- function(data, caller) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    rlang::abort(
      paste0(caller, " requires the optional arrow package"),
      class = c("fabric_arrow_error", "fabric_error")
    )
  }

  value <- data
  if (inherits(value, "data.frame")) {
    unsupported <- vapply(
      value,
      function(column) {
        is.complex(column) ||
          inherits(column, "difftime") ||
          is.environment(column) ||
          is.function(column) ||
          is.language(column)
      },
      logical(1)
    )
    if (any(unsupported)) {
      rlang::abort(paste0(
        "Unsupported column type in: ",
        paste(names(value)[unsupported], collapse = ", "),
        ". Complex and difftime columns need an explicit supported conversion"
      ))
    }
    value[] <- lapply(value, function(column) {
      if (is.factor(column)) as.character(column) else column
    })
  }

  reader <- tryCatch(
    arrow::as_record_batch_reader(value),
    error = function(error) {
      rlang::abort(
        paste0(
          "data must be a data frame, tibble, Arrow Table, RecordBatch, ",
          "Dataset, Scanner, RecordBatchReader, arrow_dplyr_query, or ",
          "Arrow-compatible array stream"
        ),
        class = c("fabric_arrow_error", "fabric_error"),
        parent = error
      )
    }
  )
  schema <- try(reader$schema, silent = TRUE)
  column_names <- if (inherits(schema, "try-error")) {
    character()
  } else {
    try(schema$names, silent = TRUE)
  }
  if (
    inherits(schema, "try-error") ||
      inherits(column_names, "try-error") ||
      !inherits(schema, "Schema")
  ) {
    rlang::abort(
      "Could not inspect the supplied Arrow schema",
      class = c("fabric_arrow_error", "fabric_error")
    )
  }
  list(
    reader = reader,
    schema = schema,
    names = as.character(column_names)
  )
}

# Stream prepared record batches into one Parquet file. Returns exact row and
# byte counts while keeping memory bounded by Arrow's current record batch.
.fabric_parquet_write_stream <- function(
  prepared,
  path,
  compression,
  caller,
  error_class
) {
  output <- NULL
  writer <- NULL
  writer_closed <- FALSE
  output_closed <- FALSE
  on.exit({
    if (!is.null(writer) && !writer_closed) {
      try(writer$Close(), silent = TRUE)
    }
    if (!is.null(output) && !output_closed) {
      try(output$close(), silent = TRUE)
    }
  }, add = TRUE)

  tryCatch(
    {
      properties <- arrow::ParquetWriterProperties$create(
        column_names = prepared$names,
        compression = compression
      )
      output <- arrow::FileOutputStream$create(path)
      writer <- arrow::ParquetFileWriter$create(
        prepared$schema,
        output,
        properties
      )
      rows <- 0
      repeat {
        batch <- prepared$reader$read_next_batch()
        if (is.null(batch)) {
          break
        }
        batch_rows <- as.numeric(batch$num_rows)
        if (
          length(batch_rows) != 1L ||
            is.na(batch_rows) ||
            !is.finite(batch_rows) ||
            batch_rows < 0
        ) {
          rlang::abort("Arrow returned an invalid record-batch row count")
        }
        if (batch_rows > 0) {
          writer$WriteBatch(
            batch,
            chunk_size = as.integer(min(batch_rows, 1024^2))
          )
        }
        rows <- rows + batch_rows
      }
      writer$Close()
      writer_closed <- TRUE
      output$close()
      output_closed <- TRUE
      bytes <- file.info(path)$size
      if (
        length(bytes) != 1L ||
          is.na(bytes) ||
          !is.finite(bytes) ||
          bytes < 0
      ) {
        rlang::abort("Arrow did not create a readable Parquet file")
      }
      list(
        path = path,
        rows = as.numeric(rows),
        bytes = as.numeric(bytes),
        names = prepared$names
      )
    },
    error = function(error) {
      rlang::abort(
        paste0("Could not serialize `data` to Parquet for ", caller),
        class = unique(c(error_class, "fabric_arrow_error", "fabric_error")),
        parent = error
      )
    }
  )
}

# Require a usable Parquet identity-mapping schema without imposing a
# destination-specific naming grammar.
.fabric_parquet_column_names <- function(value) {
  if (!length(value)) {
    rlang::abort("data must contain at least one column")
  }
  if (anyNA(value) || !all(nzchar(value))) {
    rlang::abort("data column names must be non-empty")
  }
  if (anyDuplicated(value)) {
    rlang::abort("data column names must be unique")
  }
  invisible(value)
}
