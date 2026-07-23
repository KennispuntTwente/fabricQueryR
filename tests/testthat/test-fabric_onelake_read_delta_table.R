test_that("Lakehouse item identifiers are normalized for OneLake", {
  id <- "ac3c729b-c131-46d2-adff-aec92a1a3217"

  expect_equal(fabric_normalize_lakehouse_item(id), id)
  expect_equal(
    fabric_normalize_lakehouse_item("TestLakehouse"),
    "TestLakehouse.Lakehouse"
  )
  expect_equal(
    fabric_normalize_lakehouse_item("TestLakehouse.lakehouse"),
    "TestLakehouse.lakehouse"
  )
})
