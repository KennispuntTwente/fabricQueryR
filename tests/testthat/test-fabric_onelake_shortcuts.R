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
        path = "Files/team data",
        name = "resume 2026"
      ),
      url = req$url
    )
  })

  result <- fabric_onelake_shortcut_get(
    shortcut_test_item(),
    path = "Files/team data",
    name = "resume 2026",
    token = "test-token"
  )

  expect_equal(nrow(result), 1L)
  expect_equal(result$name, "resume 2026")
  expect_match(
    request_url,
    "Files/team%20data/resume%202026",
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

test_that("bulk shortcut creation submits validated transforms as an LRO", {
  request <- NULL
  operation_id <- "66666666-6666-4666-8666-666666666666"
  httr2::local_mocked_responses(function(req) {
    request <<- req
    shortcut_test_response(
      status = 202L,
      headers = list(
        Location = paste0(
          "https://api.fabric.microsoft.com/v1/operations/",
          operation_id
        ),
        `x-ms-operation-id` = operation_id,
        `Retry-After` = "3"
      ),
      url = req$url
    )
  })

  operation <- fabric_onelake_shortcuts_bulk_create(
    shortcut_test_item(),
    shortcuts = list(
      list(
        path = "Files/imports",
        name = "orders",
        target = shortcut_test_target(),
        target_path = "Files/csv/orders",
        transform = list(
          type = "CSVTODelta",
          includeSubfolders = TRUE,
          properties = list(
            delimiter = "|",
            skipFilesWithErrors = FALSE,
            useFirstRowAsHeader = TRUE
          )
        )
      ),
      list(
        path = "Files/imports",
        name = "customers",
        target = list(
          adlsGen2 = list(
            connectionId = shortcut_test_connection_id,
            location = "https://account.dfs.core.windows.net",
            subpath = "container/customers"
          )
        )
      )
    ),
    conflict_policy = "GenerateUniqueName",
    token = "test-token"
  )
  body <- request$body$data$createShortcutRequests

  expect_s3_class(operation, "fabric_operation")
  expect_identical(operation$id, operation_id)
  expect_identical(operation$retry_after, 3)
  expect_equal(request$method, "POST")
  expect_match(request$url, "/shortcuts/bulkCreate[?]")
  expect_match(
    utils::URLdecode(request$url),
    "shortcutConflictPolicy=GenerateUniqueName",
    fixed = TRUE
  )
  expect_length(body, 2L)
  expect_equal(body[[1L]]$target$oneLake$path, "Files/csv/orders")
  expect_equal(body[[1L]]$transform$type, "csvToDelta")
  expect_true(body[[1L]]$transform$includeSubfolders)
  expect_equal(body[[1L]]$transform$properties$delimiter, "|")
  expect_named(body[[2L]]$target, "adlsGen2")
})

test_that("bulk shortcut creation rejects malformed requests locally", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    shortcut_test_response(url = req$url)
  })
  invoke <- function(shortcuts) {
    fabric_onelake_shortcuts_bulk_create(
      shortcut_test_item(),
      shortcuts = shortcuts,
      token = "test-token"
    )
  }
  valid <- list(
    path = "Files/imports",
    name = "orders",
    target = shortcut_test_target(),
    target_path = "Files/csv/orders"
  )

  for (shortcuts in list(NULL, list(), list("not-a-request"))) {
    error <- rlang::catch_cnd(invoke(shortcuts))
    expect_s3_class(error, "fabric_shortcut_bulk_error")
  }
  error <- rlang::catch_cnd(invoke(list(valid, valid)))
  expect_s3_class(error, "fabric_shortcut_bulk_error")
  expect_match(conditionMessage(error), "duplicate path and name")

  missing_target <- valid
  missing_target$target <- NULL
  error <- rlang::catch_cnd(invoke(list(missing_target)))
  expect_s3_class(error, "fabric_shortcut_bulk_error")
  expect_match(conditionMessage(error), "requires path, name, and target")

  bad_field <- valid
  bad_field$unexpected <- TRUE
  error <- rlang::catch_cnd(invoke(list(bad_field)))
  expect_s3_class(error, "fabric_shortcut_bulk_error")
  expect_match(conditionMessage(error), "unsupported field unexpected")

  invalid_transforms <- list(
    list(properties = list()),
    list(type = "parquetToDelta"),
    list(type = "csvToDelta", includeSubfolders = NA),
    list(type = "csvToDelta", properties = list(delimiter = ":")),
    list(type = "csvToDelta", properties = list(futureOption = TRUE))
  )
  for (transform in invalid_transforms) {
    request <- valid
    request$transform <- transform
    error <- rlang::catch_cnd(invoke(list(request)))
    expect_s3_class(error, "fabric_shortcut_transform_error")
  }
  expect_equal(calls, 0L)
})

test_that("shortcut cache reset uses the workspace-wide LRO endpoint", {
  request <- NULL
  operation_id <- "77777777-7777-4777-8777-777777777777"
  httr2::local_mocked_responses(function(req) {
    request <<- req
    shortcut_test_response(
      status = 202L,
      headers = list(
        Location = paste0(
          "https://api.fabric.microsoft.com/v1/operations/",
          operation_id
        ),
        `x-ms-operation-id` = operation_id,
        `Retry-After` = "5"
      ),
      url = req$url
    )
  })
  workspace <- structure(
    list(
      id = shortcut_test_workspace_id,
      displayName = "Workspace",
      type = "Workspace"
    ),
    class = c("fabric_workspace", "list")
  )

  operation <- fabric_onelake_shortcut_cache_reset(
    workspace,
    token = "test-token"
  )

  expect_s3_class(operation, "fabric_operation")
  expect_identical(operation$id, operation_id)
  expect_false(operation$result_expected)
  expect_identical(operation$retry_after, 5)
  expect_equal(request$method, "POST")
  expect_match(
    request$url,
    paste0(
      "/workspaces/",
      shortcut_test_workspace_id,
      "/onelake/resetShortcutCache$"
    )
  )
  expect_null(request$body)
})

test_that("raw OneLake shortcut targets preserve optional connection IDs", {
  target <- .fabric_shortcut_raw_target(list(
    oneLake = list(
      workspaceId = shortcut_test_target_workspace_id,
      itemId = shortcut_test_target_item_id,
      path = "Tables/dbo/orders",
      connectionId = shortcut_test_connection_id
    )
  ))

  expect_identical(
    target$oneLake$connectionId,
    shortcut_test_connection_id
  )
  expect_error(
    .fabric_shortcut_raw_target(list(
      oneLake = list(
        workspaceId = shortcut_test_target_workspace_id,
        itemId = shortcut_test_target_item_id,
        path = "Tables/dbo/orders",
        connectionId = "not-a-guid"
      )
    )),
    "OneLake target connection ID",
    fixed = TRUE
  )
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
  idempotent <- NULL
  accepted_status <- NULL
  local_mocked_bindings(
    .httr2_perform = function(
      req,
      idempotent,
      accepted_status,
      ...
    ) {
      calls <<- calls + 1L
      request <<- req
      idempotent <<- idempotent
      accepted_status <<- accepted_status
      shortcut_test_response(status = 404L, url = req$url)
    }
  )

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
  expect_false(idempotent)
  expect_identical(accepted_status, 404L)
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
  for (name in c("percent%name", "plus+name", "résumé")) {
    expect_error(invoke(name = name), "invalid path component", fixed = TRUE)
  }
  for (path in c("Files/percent%path", "Files/plus+path", "Files/équipe")) {
    expect_error(invoke(path = path), "invalid path component", fixed = TRUE)
  }
  for (target_path in c(
    "Tables/percent%path",
    "Tables/plus+path",
    "Tables/équipe"
  )) {
    expect_error(
      invoke(target_path = target_path),
      "invalid path component",
      fixed = TRUE
    )
  }
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
  for (field in c("accountKey", "secretAccessKey", "connectionString")) {
    details <- list(
      connectionId = shortcut_test_connection_id,
      location = "https://account.dfs.core.windows.net"
    )
    details[[field]] <- "credential-value"
    error <- rlang::catch_cnd(invoke(
      target = list(adlsGen2 = details),
      target_path = NULL
    ))
    expect_s3_class(error, "fabric_shortcut_target_error")
    expect_match(
      conditionMessage(error),
      "must not contain credential fields",
      fixed = TRUE
    )
  }
  details <- list(
    connectionId = shortcut_test_connection_id,
    location = "https://account.dfs.core.windows.net",
    futureOption = "unexpected"
  )
  error <- rlang::catch_cnd(invoke(
    target = list(adlsGen2 = details),
    target_path = NULL
  ))
  expect_s3_class(error, "fabric_shortcut_target_error")
  expect_match(conditionMessage(error), "unsupported field futureOption")
  expect_error(
    invoke(
      target = list(adlsGen2 = list()),
      target_path = NULL
    ),
    "requires connectionId, location"
  )
  expect_error(
    invoke(
      target = list(
        amazonS3 = list(
          connectionId = "not-a-guid",
          location = "https://bucket.s3.example.com"
        )
      ),
      target_path = NULL
    ),
    "shortcut connection ID"
  )
  expect_error(
    invoke(
      target = list(
        azureBlobStorage = list(
          connectionId = shortcut_test_connection_id,
          location = "http://account.blob.core.windows.net"
        )
      ),
      target_path = NULL
    ),
    "must be an HTTPS URL"
  )
  expect_error(
    invoke(
      target = list(
        s3Compatible = list(
          connectionId = shortcut_test_connection_id,
          location = "https://storage.example.com"
        )
      ),
      target_path = NULL
    ),
    "requires connectionId, location, bucket"
  )
  expect_error(
    invoke(
      target = list(
        oneDriveSharePoint = list(
          connectionId = shortcut_test_connection_id,
          location = "https://example.sharepoint.com",
          updateFabricItemSensitivity = "yes"
        )
      ),
      target_path = NULL
    ),
    "must be TRUE or FALSE"
  )
  expect_error(
    invoke(
      target = list(
        dataverse = list(
          connectionId = shortcut_test_connection_id,
          environmentDomain = "https://org.crm.dynamics.com",
          tableName = "account"
        )
      ),
      target_path = NULL
    ),
    "requires connectionId, deltaLakeFolder, environmentDomain, tableName"
  )
  expect_error(
    .fabric_shortcut_tibble(list(list(name = "missing path"))),
    class = "fabric_shortcut_protocol_error"
  )
  expect_equal(calls, 0L)
})

test_that("shortcut records redact credential fields returned by Fabric", {
  record <- shortcut_test_onelake_record()
  record$target$oneLake$accountKey <- "returned-secret"
  record$diagnostics <- list(connectionString = "returned-connection")

  result <- .fabric_shortcut_tibble(list(record))

  expect_equal(result$target[[1L]]$oneLake$accountKey, "<redacted>")
  expect_equal(result$raw[[1L]]$diagnostics$connectionString, "<redacted>")
  expect_false(grepl(
    "returned-secret|returned-connection",
    paste(capture.output(str(result)), collapse = "\n")
  ))
})
