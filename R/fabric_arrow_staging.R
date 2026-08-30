# Prepare an R or Arrow object for one-pass Parquet serialization. Returns a
# RecordBatchReader so lazy Arrow inputs are never collected into an R object
.fabric_parquet_prepare_data <- function(data, caller) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    .fabric_abort(
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
      .fabric_abort(paste0(
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
      .fabric_abort(
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
    .fabric_abort(
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
# byte counts while keeping memory bounded by Arrow's current record batch
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
  on.exit(
    {
      if (!is.null(writer) && !writer_closed) {
        try(writer$Close(), silent = TRUE)
      }
      if (!is.null(output) && !output_closed) {
        try(output$close(), silent = TRUE)
      }
    },
    add = TRUE
  )

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
          .fabric_abort("Arrow returned an invalid record-batch row count")
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
        .fabric_abort("Arrow did not create a readable Parquet file")
      }
      list(
        path = path,
        rows = as.numeric(rows),
        bytes = as.numeric(bytes),
        names = prepared$names
      )
    },
    error = function(error) {
      .fabric_abort(
        paste0("Could not serialize `data` to Parquet for ", caller),
        class = unique(c(error_class, "fabric_arrow_error", "fabric_error")),
        parent = error
      )
    }
  )
}

# Stream prepared record batches into a directory of bounded Parquet files
# The byte target is soft because the active row group is completed before a
# file rotates; max_rows_per_file provides an exact deterministic boundary
.fabric_parquet_write_dataset <- function(
  prepared,
  directory,
  compression,
  target_file_size,
  max_rows_per_file = NULL,
  max_files = 10000L,
  caller,
  error_class
) {
  .fabric_parquet_positive_whole(
    target_file_size,
    "target_file_size"
  )
  .fabric_parquet_positive_whole(
    max_rows_per_file,
    "max_rows_per_file",
    allow_null = TRUE
  )
  .fabric_parquet_positive_whole(max_files, "max_files")
  if (!dir.exists(directory) && !dir.create(directory, recursive = TRUE)) {
    .fabric_abort("Could not create the local Parquet staging directory")
  }
  if (!dir.exists(directory)) {
    .fabric_abort("Local Parquet staging path is not a directory")
  }

  output <- NULL
  writer <- NULL
  current_path <- NULL
  current_rows <- 0
  paths <- character()
  rows_per_file <- numeric()
  bytes_per_file <- numeric()
  buffer_bytes_per_file <- numeric()
  current_buffer_bytes <- 0
  complete <- FALSE
  on.exit(
    {
      if (!is.null(writer)) {
        try(writer$Close(), silent = TRUE)
      }
      if (!is.null(output)) {
        try(output$close(), silent = TRUE)
      }
      if (!complete) {
        unlink(unique(c(paths, current_path)), force = TRUE)
      }
    },
    add = TRUE
  )

  tryCatch(
    {
      properties <- arrow::ParquetWriterProperties$create(
        column_names = prepared$names,
        compression = compression
      )
      open_file <- function() {
        file_index <- length(paths) + 1L
        if (file_index > max_files) {
          .fabric_abort(sprintf(
            paste0(
              "Parquet staging requires more than the allowed %d files; ",
              "increase target_file_size or max_rows_per_file"
            ),
            as.integer(max_files)
          ))
        }
        current_path <<- file.path(
          directory,
          sprintf("part-%05d.parquet", file_index)
        )
        if (file.exists(current_path)) {
          .fabric_abort(
            "Parquet staging would overwrite an existing local file"
          )
        }
        output <<- arrow::FileOutputStream$create(current_path)
        writer <<- arrow::ParquetFileWriter$create(
          prepared$schema,
          output,
          properties
        )
        current_rows <<- 0
        current_buffer_bytes <<- 0
      }
      close_file <- function() {
        writer$Close()
        writer <<- NULL
        output$close()
        output <<- NULL
        bytes <- file.info(current_path)$size
        if (
          length(bytes) != 1L ||
            is.na(bytes) ||
            !is.finite(bytes) ||
            bytes < 0
        ) {
          .fabric_abort("Arrow did not create a readable Parquet part")
        }
        paths <<- c(paths, current_path)
        rows_per_file <<- c(rows_per_file, current_rows)
        bytes_per_file <<- c(bytes_per_file, as.numeric(bytes))
        buffer_bytes_per_file <<- c(
          buffer_bytes_per_file,
          current_buffer_bytes
        )
        current_path <<- NULL
        current_rows <<- 0
        current_buffer_bytes <<- 0
      }

      total_rows <- 0
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
          .fabric_abort("Arrow returned an invalid record-batch row count")
        }
        offset <- 0
        while (offset < batch_rows) {
          if (is.null(writer)) {
            open_file()
          }
          row_limit <- if (is.null(max_rows_per_file)) {
            Inf
          } else {
            max_rows_per_file - current_rows
          }
          take <- min(
            batch_rows - offset,
            row_limit,
            65536
          )
          if (!is.finite(take) || take < 1) {
            close_file()
            next
          }
          piece <- batch$Slice(as.numeric(offset), as.numeric(take))
          writer$WriteBatch(piece, chunk_size = as.integer(take))
          piece_bytes <- as.numeric(piece$nbytes())
          if (
            length(piece_bytes) != 1L ||
              is.na(piece_bytes) ||
              !is.finite(piece_bytes) ||
              piece_bytes < 0
          ) {
            .fabric_abort("Arrow returned an invalid record-batch byte size")
          }
          current_rows <- current_rows + take
          current_buffer_bytes <- current_buffer_bytes + piece_bytes
          total_rows <- total_rows + take
          offset <- offset + take
          current_size <- as.numeric(output$tell())
          rotate <- current_size >= target_file_size ||
            (!is.null(max_rows_per_file) &&
              current_rows >= max_rows_per_file)
          if (rotate) {
            close_file()
          }
        }
      }
      if (is.null(writer) && !length(paths)) {
        open_file()
      }
      if (!is.null(writer)) {
        close_file()
      }
      complete <- TRUE
      list(
        paths = paths,
        rows = as.numeric(total_rows),
        rows_per_file = rows_per_file,
        bytes = bytes_per_file,
        total_bytes = sum(bytes_per_file),
        buffer_bytes = buffer_bytes_per_file,
        total_buffer_bytes = sum(buffer_bytes_per_file),
        file_count = length(paths),
        names = prepared$names
      )
    },
    error = function(error) {
      .fabric_abort(
        paste0("Could not serialize data to partitioned Parquet for ", caller),
        class = unique(c(error_class, "fabric_arrow_error", "fabric_error")),
        parent = error
      )
    }
  )
}

.fabric_parquet_positive_whole <- function(
  value,
  name,
  allow_null = FALSE
) {
  if (is.null(value) && allow_null) {
    return(invisible(value))
  }
  if (
    !is.numeric(value) ||
      length(value) != 1L ||
      is.na(value) ||
      !is.finite(value) ||
      value < 1 ||
      value != floor(value)
  ) {
    .fabric_abort(paste0(
      name,
      " must be ",
      if (allow_null) "NULL or " else "",
      "one positive whole number"
    ))
  }
  invisible(value)
}

# Require a usable Parquet identity-mapping schema without imposing a
# destination-specific naming grammar
.fabric_parquet_column_names <- function(value) {
  if (!length(value)) {
    .fabric_abort("data must contain at least one column")
  }
  if (anyNA(value) || !all(nzchar(value))) {
    .fabric_abort("data column names must be non-empty")
  }
  if (anyDuplicated(value)) {
    .fabric_abort("data column names must be unique")
  }
  invisible(value)
}
