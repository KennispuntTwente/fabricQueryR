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
