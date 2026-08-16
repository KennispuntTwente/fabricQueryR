shortcut_test_workspace_id <- "11111111-1111-4111-8111-111111111111"
shortcut_test_item_id <- "22222222-2222-4222-8222-222222222222"
shortcut_test_target_workspace_id <- "33333333-3333-4333-8333-333333333333"
shortcut_test_target_item_id <- "44444444-4444-4444-8444-444444444444"
shortcut_test_connection_id <- "55555555-5555-4555-8555-555555555555"

shortcut_test_item <- function() {
  structure(
    list(
      id = shortcut_test_item_id,
      workspaceId = shortcut_test_workspace_id,
      displayName = "Destination",
      type = "Lakehouse"
    ),
    class = c("fabric_item", "list")
  )
}

shortcut_test_target <- function() {
  structure(
    list(
      id = shortcut_test_target_item_id,
      workspaceId = shortcut_test_target_workspace_id,
      displayName = "Source",
      type = "Warehouse"
    ),
    class = c("fabric_item", "list")
  )
}

shortcut_test_response <- function(
  body = NULL,
  status = 200L,
  url = "https://api.fabric.microsoft.com/v1/shortcuts"
) {
  raw_body <- if (is.null(body)) {
    raw()
  } else {
    charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE, null = "null"))
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = list(`content-type` = "application/json"),
    body = raw_body
  )
}

shortcut_test_onelake_record <- function(
  path = "Files/shared",
  name = "orders",
  transform = FALSE
) {
  record <- list(
    path = path,
    name = name,
    target = list(
      type = "OneLake",
      oneLake = list(
        workspaceId = shortcut_test_target_workspace_id,
        itemId = shortcut_test_target_item_id,
        path = "Tables/dbo/orders"
      )
    )
  )
  if (transform) {
    record$isShortcutTransform <- TRUE
    record$transform <- list(type = "csvToDelta")
  }
  record
}

test_that("shortcut listing paginates and preserves heterogeneous targets", {
  calls <- character()
  httr2::local_mocked_responses(function(req) {
    calls <<- c(calls, req$url)
    decoded <- utils::URLdecode(req$url)
    body <- if (grepl("continuationToken=page-2", decoded, fixed = TRUE)) {
      list(
        value = list(list(
          path = "Files/shared",
          name = "external",
          target = list(
            type = "AdlsGen2",
            adlsGen2 = list(
              connectionId = shortcut_test_connection_id,
              location = "https://account.dfs.core.windows.net",
              subpath = "container/data"
            )
          )
        ))
      )
    } else {
      list(
        value = list(shortcut_test_onelake_record(transform = TRUE)),
        continuationToken = "page-2"
      )
    }
    shortcut_test_response(body, url = req$url)
  })

  result <- fabric_onelake_shortcuts(
    shortcut_test_item(),
    parent_path = "Files/shared",
    token = "test-token"
  )

  expect_s3_class(result, "tbl_df")
  expect_equal(nrow(result), 2L)
  expect_equal(result$name, c("orders", "external"))
  expect_equal(result$target_type, c("OneLake", "AdlsGen2"))
  expect_equal(
    result$one_lake_workspace_id,
    c(shortcut_test_target_workspace_id, NA_character_)
  )
  expect_equal(result$is_transform, c(TRUE, FALSE))
  expect_equal(result$transform[[1L]]$type, "csvToDelta")
  expect_equal(result$target[[2L]]$adlsGen2$subpath, "container/data")
  expect_length(calls, 2L)
  expect_match(
    utils::URLdecode(calls[[1L]]),
    "parentPath=Files/shared",
    fixed = TRUE
  )
  expect_match(
    utils::URLdecode(calls[[2L]]),
    "continuationToken=page-2",
    fixed = TRUE
  )
})

test_that("shortcut get encodes path components and returns one row", {
  request_url <- NULL
  httr2::local_mocked_responses(function(req) {
    request_url <<- req$url
    shortcut_test_response(
      shortcut_test_onelake_record(
        path = "Files/équipe data",
        name = "résumé 2026"
      ),
      url = req$url
    )
  })

  result <- fabric_onelake_shortcut_get(
    shortcut_test_item(),
    path = "Files/équipe data",
    name = "résumé 2026",
    token = "test-token"
  )

  expect_equal(nrow(result), 1L)
  expect_equal(result$name, "résumé 2026")
  expect_match(
    request_url,
    "Files/%C3%A9quipe%20data/r%C3%A9sum%C3%A9%202026",
    fixed = TRUE
  )
})

test_that("shortcut create builds a discovered OneLake target", {
  request <- NULL
  httr2::local_mocked_responses(function(req) {
    request <<- req
    shortcut_test_response(
      shortcut_test_onelake_record(),
      status = 201L,
      url = req$url
    )
  })

  result <- fabric_onelake_shortcut_create(
    shortcut_test_item(),
    path = "Files/shared",
    name = "orders",
    target = shortcut_test_target(),
    target_path = "Tables/dbo/orders",
    conflict_policy = "createoroverwrite",
    token = "test-token"
  )
  body <- request$body$data

  expect_equal(request$method, "POST")
  expect_match(
    utils::URLdecode(request$url),
    "shortcutConflictPolicy=CreateOrOverwrite",
    fixed = TRUE
  )
  expect_equal(body$path, "Files/shared")
  expect_equal(body$name, "orders")
  expect_named(body$target, "oneLake")
  expect_equal(
    body$target$oneLake$workspaceId,
    shortcut_test_target_workspace_id
  )
  expect_equal(body$target$oneLake$itemId, shortcut_test_target_item_id)
  expect_equal(body$target$oneLake$path, "Tables/dbo/orders")
  expect_equal(result$target_type, "OneLake")
})

test_that("shortcut create emits every non-default conflict policy", {
  request_urls <- character()
  httr2::local_mocked_responses(function(req) {
    request_urls <<- c(request_urls, utils::URLdecode(req$url))
    shortcut_test_response(
      shortcut_test_onelake_record(),
      status = 201L,
      url = req$url
    )
  })
  policies <- c(
    "Abort",
    "GenerateUniqueName",
    "CreateOrOverwrite",
    "OverwriteOnly"
  )

  for (policy in policies) {
    fabric_onelake_shortcut_create(
      shortcut_test_item(),
      path = "Files/shared",
      name = "orders",
      target = shortcut_test_target(),
      target_path = "Tables/dbo/orders",
      conflict_policy = policy,
      token = "test-token"
    )
  }

  expect_false(grepl(
    "shortcutConflictPolicy",
    request_urls[[1L]],
    fixed = TRUE
  ))
  for (index in 2:4) {
    expect_match(
      request_urls[[index]],
      paste0("shortcutConflictPolicy=", policies[[index]]),
      fixed = TRUE
    )
  }
})

test_that("shortcut create accepts documented connection-backed targets", {
  request <- NULL
  httr2::local_mocked_responses(function(req) {
    request <<- req
    shortcut_test_response(
      list(
        path = "Files",
        name = "landing",
        target = list(
          type = "AdlsGen2",
          adlsGen2 = list(
            connectionId = shortcut_test_connection_id,
            location = "https://account.dfs.core.windows.net",
            subpath = "container/landing"
          )
        )
      ),
      status = 201L,
      url = req$url
    )
  })
  raw_target <- list(
    type = "AdlsGen2",
    adlsGen2 = list(
      connectionId = shortcut_test_connection_id,
      location = "https://account.dfs.core.windows.net",
      subpath = "container/landing"
    )
  )

  result <- fabric_onelake_shortcut_create(
    shortcut_test_item(),
    path = "Files",
    name = "landing",
    target = raw_target,
    token = "test-token"
  )
  body <- request$body$data

  expect_false(grepl("shortcutConflictPolicy", request$url, fixed = TRUE))
  expect_named(body$target, "adlsGen2")
  expect_equal(body$target$adlsGen2$connectionId, shortcut_test_connection_id)
  expect_equal(result$target_type, "AdlsGen2")
})

test_that("shortcut POST is never replayed after a transient response", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    shortcut_test_response(
      list(errorCode = "ServiceUnavailable", isRetriable = TRUE),
      status = 503L,
      url = req$url
    )
  })

  expect_error(
    fabric_onelake_shortcut_create(
      shortcut_test_item(),
      path = "Files",
      name = "orders",
      target = shortcut_test_target(),
      target_path = "Tables/dbo/orders",
      token = "test-token"
    ),
    class = "fabric_http_error"
  )
  expect_equal(calls, 1L)
})

test_that("shortcut delete requires confirmation and deletes only the link", {
  calls <- 0L
  request <- NULL
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    request <<- req
    shortcut_test_response(status = 200L, url = req$url)
  })

  expect_error(
    fabric_onelake_shortcut_delete(
      shortcut_test_item(),
      "Files/shared",
      "orders",
      token = "test-token"
    ),
    "confirm = TRUE"
  )
  expect_equal(calls, 0L)
  expect_true(fabric_onelake_shortcut_delete(
    shortcut_test_item(),
    "Files/shared",
    "orders",
    confirm = TRUE,
    token = "test-token"
  ))
  expect_equal(calls, 1L)
  expect_equal(request$method, "DELETE")
  expect_match(request$url, "/shortcuts/Files/shared/orders$", perl = TRUE)
})

test_that("shortcut validation stops unsafe requests locally", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    shortcut_test_response(url = req$url)
  })
  invoke <- function(
    path = "Files",
    name = "orders",
    target = shortcut_test_target(),
    target_path = "Tables/dbo/orders",
    conflict_policy = "Abort",
    ...
  ) {
    fabric_onelake_shortcut_create(
      shortcut_test_item(),
      path = path,
      name = name,
      target = target,
      target_path = target_path,
      conflict_policy = conflict_policy,
      token = "test-token",
      ...
    )
  }

  expect_error(invoke(path = "Other/shared"), "begin with Files or Tables")
  expect_error(invoke(name = "bad/name"), "invalid path component")
  expect_error(invoke(name = "NUL"), "invalid path component")
  expect_error(invoke(conflict_policy = "replace"), "must be one of")
  expect_error(
    invoke(target_path = NULL),
    class = "fabric_shortcut_target_error"
  )
  expect_error(
    invoke(
      target = list(
        oneLake = list(),
        adlsGen2 = list()
      ),
      target_path = NULL
    ),
    "exactly one target type"
  )
  expect_error(
    invoke(
      target = list(
        adlsGen2 = list(
          connectionId = shortcut_test_connection_id,
          sasToken = "secret"
        )
      ),
      target_path = NULL
    ),
    "must not contain credential fields"
  )
  expect_error(
    .fabric_shortcut_tibble(list(list(name = "missing path"))),
    class = "fabric_shortcut_protocol_error"
  )
  expect_equal(calls, 0L)
})
