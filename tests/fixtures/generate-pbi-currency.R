# Run from the repository root with a current Arrow release. Arrow 9 can read the
# decimal IPC fixture but cannot construct it by casting a string array.
currency <- arrow::Array$create(c(
  "123456789012345.6789",
  "-0.0100",
  NA_character_
))$cast(arrow::decimal128(19, 4))
arrow::write_ipc_stream(
  arrow::arrow_table(currency = currency),
  "tests/fixtures/pbi-currency.arrows"
)
