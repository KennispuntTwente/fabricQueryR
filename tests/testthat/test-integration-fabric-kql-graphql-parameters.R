# Fabric integration coverage: kql graphql parameters
test_that("explicit dynamic timestamp and special-number strings retain values", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  database <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestKQLDatabase")$id,
    token = token
  )
  stamp <- as.POSIXct("2026-09-19 12:13:14", tz = "Europe/Amsterdam") + .123456
  result <- fabric_kql_query(
    database,
    paste(
      "declare query_parameters(moment:datetime, x:dynamic);",
      "print sameTime=moment == todatetime(x.moment),",
      "positive=toreal(x.positive) == real(+inf),",
      "negative=toreal(x.negative) == real(-inf),",
      "notNumber=isnan(toreal(x.notNumber))"
    ),
    parameters = list(
      moment = stamp,
      x = list(
        moment = "2026-09-19T10:13:14.1234560Z",
        positive = "+inf",
        negative = "-inf",
        notNumber = "nan"
      )
    ),
    token = token
  )
  expect_identical(
    lapply(result, identity),
    list(
      sameTime = TRUE,
      positive = TRUE,
      negative = TRUE,
      notNumber = TRUE
    )
  )
})

test_that("nested singleton integer64 values round trip as exact Kusto longs", {
  manifest <- fabric_test_manifest()
  fabric_test_require_package("bit64")
  token <- fabric_test_token_provider()
  database <- fabric_item(
    manifest$workspace_id,
    fabric_test_manifest_item(manifest, "TestKQLDatabase")$id,
    token = token
  )
  for (text in c("-1", "9223372036854775807", "-9223372036854775807")) {
    result <- fabric_kql_query(
      database,
      paste(
        "declare query_parameters(x:dynamic);",
        "print value=tostring(x.nested.value), type=gettype(x.nested.value)"
      ),
      parameters = list(
        x = list(nested = list(value = bit64::as.integer64(text)))
      ),
      token = token
    )
    expect_identical(result$value, text)
    expect_identical(result$type, "long")
  }
})
