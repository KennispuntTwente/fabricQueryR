# Fabric integration coverage: refresh failures, partitions, and incremental windows
test_that("partition and PartialBatch refreshes preserve untargeted partition data", {
  manifest <- fabric_test_manifest()
  partition <- function(name) {
    list(
      name = name,
      mode = "import",
      source = list(
        type = "m",
        expression = paste0(
          '#table(type table [partition = text, marker = text], {{"',
          name,
          '", DateTimeZone.ToText(DateTimeZone.UtcNow())}})'
        )
      )
    )
  }
  definition <- list(
    compatibilityLevel = 1600L,
    model = list(
      culture = "en-US",
      defaultPowerBIDataSourceVersion = "powerBI_V3",
      tables = list(list(
        name = "Marker",
        columns = list(
          list(
            name = "partition",
            dataType = "string",
            sourceColumn = "partition"
          ),
          list(name = "marker", dataType = "string", sourceColumn = "marker")
        ),
        partitions = list(partition("Current"), partition("History"))
      ))
    )
  )
  model <- fabric_test_marker_model(
    manifest$workspace_id,
    fabric_test_token_provider(),
    definition
  )
  read <- function() {
    model$dax_query("EVALUATE 'Marker' ORDER BY 'Marker'[partition]")
  }
  model$refresh_wait(
    model$refresh(mode = "enhanced", type = "Full"),
    timeout = 600
  )
  before <- read()
  expect_equal(nrow(before), 2L)
  for (commit in c("Transactional", "PartialBatch")) {
    Sys.sleep(2)
    job <- model$refresh(
      mode = "enhanced",
      type = "Full",
      commit_mode = commit,
      objects = list(list(table = "Marker", partition = "Current")),
      apply_refresh_policy = FALSE
    )
    completed <- model$refresh_wait(job, timeout = 600)
    expect_true(completed$state %in% c("Completed", "CompletedWithWarnings"))
    after <- read()
    expect_equal(nrow(after), 2L)
    expect_identical(after[[2L]][[2L]], before[[2L]][[2L]])
    expect_false(identical(after[[2L]][[1L]], before[[2L]][[1L]]))
    before <- after
  }
})

test_that("failed enhanced refreshes expose diagnostics and typed wait errors", {
  manifest <- fabric_test_manifest()
  definition <- list(
    compatibilityLevel = 1600L,
    model = list(
      culture = "en-US",
      defaultPowerBIDataSourceVersion = "powerBI_V3",
      tables = list(list(
        name = "Failure",
        columns = list(
          list(name = "id", dataType = "int64", sourceColumn = "id")
        ),
        partitions = list(list(
          name = "Failure",
          mode = "import",
          source = list(
            type = "m",
            expression = 'error "FABRICQUERYR_INTENTIONAL_REFRESH_FAILURE"'
          )
        ))
      ))
    )
  )
  model <- fabric_test_marker_model(
    manifest$workspace_id,
    fabric_test_token_provider(),
    definition
  )
  job <- model$refresh(mode = "enhanced", type = "Full", retry_count = 0L)
  failed <- model$refresh_wait(job, timeout = 600, error_on_failure = FALSE)
  expect_identical(failed$state, "Failed")
  expect_identical(failed$id, job$id)
  expect_true(length(failed$attempts) >= 1L)
  expect_match(
    jsonlite::toJSON(failed$raw, auto_unbox = TRUE),
    "FABRICQUERYR_INTENTIONAL_REFRESH_FAILURE",
    fixed = TRUE
  )
  error <- expect_error(
    model$refresh_wait(job, timeout = 60),
    class = "fabric_pbi_refresh_failed"
  )
  expect_identical(error$refresh_status$state, "Failed")
  history <- model$refresh_history(top = 10L)
  entry <- Filter(function(entry) identical(entry$id, job$id), history)[[1L]]
  expect_identical(entry$state, "Failed")
})

test_that("incremental refresh applies the effective date to partition windows", {
  manifest <- fabric_test_manifest()
  expression <- paste(
    "let Source = #table(type table [eventDate = datetime, id = Int64.Type],",
    "{{#datetime(2026, 1, 1, 12, 0, 0), 1}, {#datetime(2026, 1, 2, 12, 0, 0), 2}, {#datetime(2026, 1, 3, 12, 0, 0), 3}}),",
    "Filtered = Table.SelectRows(Source, each [eventDate] >= RangeStart and [eventDate] < RangeEnd) in Filtered"
  )
  definition <- list(
    compatibilityLevel = 1600L,
    model = list(
      culture = "en-US",
      defaultPowerBIDataSourceVersion = "powerBI_V3",
      expressions = list(
        list(
          name = "RangeStart",
          kind = "m",
          expression = "#datetime(2026, 1, 1, 0, 0, 0)"
        ),
        list(
          name = "RangeEnd",
          kind = "m",
          expression = "#datetime(2026, 1, 4, 0, 0, 0)"
        )
      ),
      tables = list(list(
        name = "Window",
        columns = list(
          list(
            name = "eventDate",
            dataType = "dateTime",
            sourceColumn = "eventDate"
          ),
          list(name = "id", dataType = "int64", sourceColumn = "id")
        ),
        refreshPolicy = list(
          policyType = "basic",
          rollingWindowGranularity = "day",
          rollingWindowPeriods = 2L,
          incrementalGranularity = "day",
          incrementalPeriods = 1L,
          sourceExpression = expression
        ),
        partitions = list(list(
          name = "Window",
          mode = "import",
          source = list(type = "m", expression = expression)
        ))
      ))
    )
  )
  model <- fabric_test_marker_model(
    manifest$workspace_id,
    fabric_test_token_provider(),
    definition
  )
  for (date in c("2026-01-03", "2026-01-04")) {
    job <- model$refresh(
      mode = "enhanced",
      type = "Full",
      apply_refresh_policy = TRUE,
      effective_date = as.Date(date),
      objects = "Window"
    )
    model$refresh_wait(job, timeout = 600)
    rows <- model$dax_query("EVALUATE 'Window' ORDER BY 'Window'[id]")
    # Fabric includes the current effective day as well as the historical window.
    expect_equal(as.integer(rows[[2L]]), if (date == "2026-01-03") 1:3 else 2:3)
  }
})
