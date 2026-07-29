#' Read a Delta table with the native delta-rs engine
#' @keywords internal
#' @noRd
fabric_onelake_read_delta_table_delta_rs <- function(context) {
  rlang::check_installed(
    "arrow",
    reason = "to materialise delta-rs Arrow output in R"
  )
  bearer_token <- fabric_get_token(
    context$credential,
    .fabric_audience$storage
  )
  uri <- fabric_delta_abfss_uri(context$target)
  ipc_path <- file.path(context$dest_dir, "snapshot.arrow")
  version <- context$version %||% -1
  limit <- context$limit %||% -1
  columns <- context$columns %||% character()

  tryCatch(
    fabric_delta_rs_read_to_ipc(
      uri = uri,
      bearer_token = bearer_token,
      version = version,
      columns = columns,
      limit = limit,
      ipc_path = ipc_path
    ),
    error = function(error) {
      fabric_delta_rs_abort(error)
    }
  )
  if (!file.exists(ipc_path)) {
    rlang::abort(
      "delta-rs completed without creating its Arrow IPC result",
      class = "fabric_delta_rs_error"
    )
  }
  arrow::read_ipc_file(ipc_path, as_data_frame = FALSE)
}

#' Translate native delta-rs failures into stable R condition classes
#' @keywords internal
#' @noRd
fabric_delta_rs_abort <- function(error) {
  message <- conditionMessage(error)
  unsupported <- grepl(
    paste(
      c(
        "UnsupportedTableFeatures",
        "Unsupported table features",
        "unsupported reader feature",
        "unsupported table feature"
      ),
      collapse = "|"
    ),
    message,
    ignore.case = TRUE
  )
  classes <- if (unsupported) {
    c(
      "fabric_delta_rs_unsupported_error",
      "fabric_delta_unsupported_error",
      "fabric_delta_rs_error"
    )
  } else {
    "fabric_delta_rs_error"
  }
  rlang::abort(
    paste0("delta-rs could not read the Delta table: ", message),
    class = classes,
    parent = error
  )
}
