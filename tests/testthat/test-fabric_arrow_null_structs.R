test_that("tibble collection refuses observed null struct parents before losing validity", {
  nested_schema <- nanoarrow::na_struct(list(value = nanoarrow::na_int32()))
  nested <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(nested_schema),
    list(
      length = 3L,
      null_count = -1L,
      buffers = list(as.raw(5L)),
      children = list(
        value = nanoarrow::as_nanoarrow_array(c(1L, NA_integer_, NA_integer_))
      )
    )
  )
  schema <- nanoarrow::na_struct(list(nested = nested_schema))
  batch <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(schema),
    list(length = 3L, children = list(nested = nested))
  )
  for (batches in list(list(batch), list(batch, batch))) {
    stream <- nanoarrow::basic_array_stream(batches, schema = schema)
    withr::defer(nanoarrow::nanoarrow_pointer_release(stream))
    error <- rlang::catch_cnd(.fabric_arrow_exact_tibble(stream))
    expect_s3_class(error, "fabric_arrow_null_struct_error")
    expect_identical(error$arrow_column, "nested")
    expect_match(error$message %||% "", 'result = "arrow_stream"', fixed = TRUE)
  }
  # A valid all-null struct remains different from a null parent and is supported.
  valid <- nanoarrow::nanoarrow_array_modify(
    nested,
    list(offset = 2L, length = 1L, null_count = 0L)
  )
  stream <- nanoarrow::basic_array_stream(list(nanoarrow::nanoarrow_array_modify(
    batch,
    list(length = 1L, children = list(nested = valid))
  )))
  withr::defer(nanoarrow::nanoarrow_pointer_release(stream))
  expect_identical(.fabric_arrow_exact_tibble(stream)$nested$value, NA_integer_)
})

test_that("null struct checks follow selected list elements and dictionary entries", {
  skip_if_not_installed("arrow")
  struct <- nanoarrow::as_nanoarrow_array(arrow::StructArray$create(
    value = c(1L, NA_integer_, NA_integer_, 4L)
  ))
  struct <- nanoarrow::nanoarrow_array_modify(
    struct,
    list(buffers = list(as.raw(13L)), null_count = 1L)
  )
  struct_schema <- nanoarrow::infer_nanoarrow_schema(struct)
  wrap <- function(array) {
    schema <- nanoarrow::na_struct(list(
      value = nanoarrow::infer_nanoarrow_schema(array)
    ))
    nanoarrow::nanoarrow_array_modify(
      nanoarrow::nanoarrow_array_init(schema),
      list(length = array$length, children = list(value = array))
    )
  }
  collect <- function(array) {
    stream <- nanoarrow::basic_array_stream(list(wrap(array)))
    on.exit(nanoarrow::nanoarrow_pointer_release(stream))
    .fabric_arrow_exact_tibble(stream)
  }
  for (large in c(FALSE, TRUE)) {
    list_schema <- if (large) {
      nanoarrow::na_large_list(struct_schema)
    } else {
      nanoarrow::na_list(struct_schema)
    }
    offsets <- if (large) {
      bit64::as.integer64(c(0, 1, 3, 4))
    } else {
      c(0L, 1L, 3L, 4L)
    }
    array <- nanoarrow::nanoarrow_array_modify(
      nanoarrow::nanoarrow_array_init(list_schema),
      list(
        length = 3L,
        buffers = list(
          raw(),
          nanoarrow::as_nanoarrow_array(offsets)$buffers[[2L]]
        ),
        children = list(struct)
      )
    )
    error <- rlang::catch_cnd(collect(array))
    expect_s3_class(error, "fabric_arrow_null_struct_error")
    sliced <- nanoarrow::nanoarrow_array_modify(
      array,
      list(offset = 2L, length = 1L)
    )
    expect_identical(collect(sliced)$value[[1L]]$value, 4)
    # A null list does not expose its backing child structs.
    masked <- nanoarrow::nanoarrow_array_modify(
      array,
      list(buffers = list(as.raw(5L), array$buffers[[2L]]), null_count = 1L)
    )
    expect_null(collect(masked)$value[[2L]])
  }
  fixed <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(nanoarrow::na_fixed_size_list(
      struct_schema,
      1L
    )),
    list(length = 4L, buffers = list(raw()), children = list(struct))
  )
  expect_s3_class(
    rlang::catch_cnd(collect(fixed)),
    "fabric_arrow_null_struct_error"
  )
  expect_identical(
    collect(nanoarrow::nanoarrow_array_modify(
      fixed,
      list(offset = 2L, length = 2L)
    ))$value[[2L]]$value,
    4
  )

  dictionary <- arrow::as_arrow_array(struct)
  sliced_dictionary <- nanoarrow::as_nanoarrow_array(
    arrow::DictionaryArray$create(
      arrow::Array$create(c(0L, 1L, 2L)),
      dictionary$Slice(1L, 3L)
    )$Slice(1L, 2L)
  )
  expect_identical(collect(sliced_dictionary)$value$value, c(NA_integer_, 4L))
  for (indices in list(c(0L, 2L, 3L), c(1L), c(NA_integer_, 0L))) {
    array <- nanoarrow::as_nanoarrow_array(arrow::DictionaryArray$create(
      arrow::Array$create(indices),
      dictionary
    ))
    if (identical(indices, c(0L, 2L, 3L))) {
      expect_identical(collect(array)$value$value, c(1L, NA_integer_, 4L))
    } else {
      expect_s3_class(
        rlang::catch_cnd(collect(array)),
        "fabric_arrow_null_struct_error"
      )
    }
  }
  key_schema <- nanoarrow::nanoarrow_schema_modify(
    nanoarrow::na_string(),
    list(flags = 0L)
  )
  entries_schema <- nanoarrow::nanoarrow_schema_modify(
    nanoarrow::na_struct(list(key = key_schema, value = struct_schema)),
    list(flags = 0L)
  )
  entries <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(entries_schema),
    list(
      length = 4L,
      children = list(
        key = nanoarrow::nanoarrow_array_set_schema(
          nanoarrow::as_nanoarrow_array(letters[1:4]),
          key_schema
        ),
        value = struct
      )
    )
  )
  map_schema <- nanoarrow::nanoarrow_schema_modify(
    nanoarrow::na_list(entries_schema),
    list(format = "+m")
  )
  map <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(map_schema),
    list(
      length = 2L,
      buffers = list(raw(), c(0L, 2L, 4L)),
      children = list(entries)
    )
  )
  expect_s3_class(
    rlang::catch_cnd(collect(map)),
    "fabric_arrow_null_struct_error"
  )
  selected <- nanoarrow::nanoarrow_array_modify(
    map,
    list(offset = 1L, length = 1L)
  )
  expect_identical(collect(selected)$value[[1L]]$value$value, c(NA_real_, 4))
})

test_that("unhandled struct container layouts fail before tibble conversion", {
  child <- nanoarrow::as_nanoarrow_array(data.frame(value = 1L))
  schema <- nanoarrow::na_dense_union(list(
    value = nanoarrow::infer_nanoarrow_schema(child)
  ))
  array <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(schema),
    list(
      length = 1L,
      buffers = list(as.raw(0L), 0L),
      children = list(child)
    )
  )
  error <- rlang::catch_cnd(.fabric_arrow_validate_struct_collection(array))
  expect_s3_class(error, "fabric_arrow_struct_layout_error")
  expect_identical(error$arrow_format, "+ud:0")
})

test_that("OneLake Arrow streams retain struct masks through reorder and multiple-batch writes", {
  skip_if_not_installed("arrow")
  store <- new.env(parent = emptyenv())
  withr::defer(unlink(unlist(as.list(store)), force = TRUE))
  local_mocked_bindings(
    fabric_onelake_upload = function(path, source, ...) {
      target <- tempfile(fileext = paste0(".", tools::file_ext(path)))
      file.copy(source, target)
      store[[path]] <- target
      tibble::tibble(path = path)
    },
    fabric_onelake_download = function(path, dest, ...) {
      file.copy(store[[path]], dest)
      invisible(dest)
    }
  )
  values <- c(-0, NA_real_, NA_real_, pi)
  nested <- nanoarrow::as_nanoarrow_array(arrow::StructArray$create(
    amount = arrow::Array$create(c(
      "9007199254740993",
      NA,
      NA,
      "9223372036854775807"
    ))$cast(arrow::int64()),
    value = values,
    empty = arrow::Array$create(rep(NA, 4L), type = arrow::null())
  ))
  nested <- nanoarrow::nanoarrow_array_modify(
    nested,
    list(buffers = list(as.raw(13L)), null_count = 1L)
  )
  schema <- nanoarrow::na_struct(list(
    nested = nanoarrow::infer_nanoarrow_schema(nested)
  ))
  batch <- nanoarrow::nanoarrow_array_modify(
    nanoarrow::nanoarrow_array_init(schema),
    list(length = 4L, children = list(nested = nested))
  )
  for (format in c("parquet", "arrow")) {
    source <- nanoarrow::basic_array_stream(list(batch, batch), schema = schema)
    path <- paste0("Files/struct-original.", format)
    fabric_onelake_write_file("workspace", "item", path, source)
    error <- rlang::catch_cnd(fabric_onelake_read_file(
      "workspace",
      "item",
      path
    ))
    expect_s3_class(error, "fabric_arrow_null_struct_error")
    stream <- fabric_onelake_read_file(
      "workspace",
      "item",
      path,
      result = "arrow_stream"
    )
    reader <- arrow::as_record_batch_reader(stream)
    table <- reader$read_table()
    reader$Close()
    selected <- c(3L, 1L, 2L, 0L, 7L, 5L, 6L, 4L)
    reordered <- table$Take(arrow::Array$create(selected))
    output <- paste0("Files/struct-reordered.", format)
    fabric_onelake_write_file("workspace", "item", output, reordered)
    stream <- fabric_onelake_read_file(
      "workspace",
      "item",
      output,
      result = "arrow_stream"
    )
    reader <- arrow::as_record_batch_reader(stream)
    actual <- do.call(
      arrow::concat_arrays,
      reader$read_table()$column(0)$chunks
    )
    reader$Close()
    expect_identical(
      vapply(0:7, actual$IsNull, logical(1)),
      rep(c(FALSE, TRUE, FALSE, FALSE), 2L)
    )
    expect_identical(
      writeBin(actual$GetFieldByName("value")$as_vector(), raw(), 8L),
      writeBin(rep(values, 2L)[selected + 1L], raw(), 8L)
    )
    expect_identical(
      as.character(actual$GetFieldByName("amount")$cast(arrow::utf8())),
      rep(c("9223372036854775807", NA, NA, "9007199254740993"), 2L)
    )
  }
})
