# Fabric integration coverage: OneLake nested struct null preservation
# The test round-trips parent-null Arrow structs through live OneLake storage
# and verifies stream fidelity plus the guarded tibble conversion

test_that("OneLake struct null masks survive stream round trips and tibble reads refuse loss", {
  fabric_test_require_package("arrow")
  manifest <- fabric_test_manifest()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  token <- fabric_test_token_provider()
  item <- fabric_item(
    manifest$workspace_id,
    lakehouse$id,
    type = "Lakehouse",
    token = token
  )
  object <- fabric_r6_record(
    as.list(item),
    legacy_class = c("fabric_item", "list")
  )
  root <- paste0(
    "Files/fabricqueryr-tests/struct-validity-",
    format(Sys.time(), "%Y%m%d%H%M%S"),
    "-",
    Sys.getpid()
  )
  on.exit(
    fabric_onelake_delete(
      manifest$workspace_id,
      lakehouse$id,
      root,
      recursive = TRUE,
      confirm = TRUE,
      token = token
    ),
    add = TRUE
  )
  values <- c(-0, NA_real_, NA_real_, pi)
  nested <- nanoarrow::as_nanoarrow_array(arrow::StructArray$create(
    amount = arrow::Array$create(c(
      "9007199254740993",
      NA,
      NA,
      "9223372036854775807"
    ))$cast(arrow::int64()),
    value = values
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
    path <- paste0(root, "/source.", format)
    source <- nanoarrow::basic_array_stream(list(batch, batch), schema = schema)
    object$onelake_write_file(path, source, token = token)
    error <- rlang::catch_cnd(
      fabric_onelake_read_file(
        manifest$workspace_id,
        lakehouse$id,
        path,
        token = token
      ),
      classes = "error"
    )
    expect_s3_class(error, "fabric_arrow_null_struct_error")
    error <- rlang::catch_cnd(
      object$onelake_read_file(path, token = token),
      classes = "error"
    )
    expect_s3_class(error, "fabric_arrow_null_struct_error")
    stream <- object$onelake_read_file(
      path,
      result = "arrow_stream",
      token = token
    )
    reader <- arrow::as_record_batch_reader(stream)
    table <- reader$read_table()
    reader$Close()
    selected <- c(3L, 1L, 2L, 0L, 7L, 5L, 6L, 4L)
    target <- paste0(root, "/reordered.", format)
    fabric_onelake_write_file(
      manifest$workspace_id,
      lakehouse$id,
      target,
      table$Take(arrow::Array$create(selected)),
      token = token
    )
    stream <- fabric_onelake_read_file(
      manifest$workspace_id,
      lakehouse$id,
      target,
      result = "arrow_stream",
      token = token
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
