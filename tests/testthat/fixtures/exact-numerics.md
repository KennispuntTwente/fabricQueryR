`exact-numerics.arrow` is a synthetic two-row Arrow IPC stream. It keeps test
construction independent of newer Arrow string-to-decimal casts and struct
constructors, allowing the conversion tests to run with Arrow 9.

The first row contains decimal128(24,4) `12345678901234567890.1234`, int32
`-2147483648`, int64 `-9223372036854775808`, and int64 `2147483648`. The second
row contains nulls. The minimum int64 also appears in a struct, dictionary,
and list column; the second list is empty and the first includes a null.

Generate with a recent Arrow R release:

```r
scalar <- arrow::Array$create(c("-9223372036854775808", NA))$cast(arrow::int64())
table <- arrow::Table$create(
  amount = arrow::Array$create(c("12345678901234567890.1234", NA))$cast(arrow::decimal128(24, 4)),
  i32 = arrow::Array$create(c("-2147483648", NA))$cast(arrow::int32()),
  i64 = scalar,
  large = arrow::Array$create(c("2147483648", NA))$cast(arrow::int64()),
  nested = arrow::StructArray$create(value = scalar),
  dictionary = arrow::call_function("dictionary_encode", scalar),
  list = arrow::Array$create(
    list(c("-9223372036854775808", NA), character()),
    type = arrow::list_of(arrow::utf8())
  )$cast(arrow::list_of(arrow::int64()))
)
arrow::write_ipc_stream(table, "tests/testthat/fixtures/exact-numerics.arrow")
```
