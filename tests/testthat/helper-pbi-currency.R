pbi_test_currency_array <- function() {
  arrow::read_ipc_stream(
    test_path("..", "fixtures", "pbi-currency.arrows"),
    as_data_frame = FALSE
  )$column(0L)$chunk(0L)
}
