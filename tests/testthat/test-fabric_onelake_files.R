onelake_test_response <- function(
  status = 200L,
  body = raw(),
  headers = list(),
  url = "https://onelake.dfs.fabric.microsoft.com"
) {
  if (is.list(body)) {
    body <- charToRaw(jsonlite::toJSON(body, auto_unbox = TRUE))
    headers[["content-type"]] <- "application/json"
  }
  httr2::response(
    status_code = status,
    url = url,
    headers = headers,
    body = body
  )
}

test_that("OneLake targets support IDs, discovery records, and complete paths", {
  workspace_id <- "11111111-1111-1111-1111-111111111111"
  item_id <- "22222222-2222-2222-2222-222222222222"

  named <- onelake_resolve_target(
    "Analytics",
    "Curated",
    "Files/café 数据.csv",
    "Lakehouse"
  )
  expect_equal(named$workspace, "Analytics")
  expect_equal(named$item, "Curated.Lakehouse")
  expect_match(onelake_path_url(named), "caf%C3%A9%20%E6%95%B0%E6%8D%AE.csv")
  expect_equal(
    onelake_resolve_target(
      "Analytics",
      "Curated.v2",
      item_type = "Lakehouse"
    )$item,
    "Curated.v2.Lakehouse"
  )

  discovered <- onelake_resolve_target(
    NULL,
    list(id = item_id, workspaceId = workspace_id, type = "Lakehouse"),
    "Files/nested/file.csv"
  )
  expect_equal(discovered$workspace, workspace_id)
  expect_equal(discovered$item, item_id)

  https <- onelake_resolve_target(paste0(
    "https://onelake.dfs.fabric.microsoft.com/",
    workspace_id,
    "/",
    item_id,
    "/Files/nested/file.csv"
  ))
  abfss <- onelake_resolve_target(paste0(
    "abfss://",
    workspace_id,
    "@onelake.dfs.fabric.microsoft.com/",
    item_id,
    "/Files/nested/file.csv"
  ))
  expect_equal(
    https[c("workspace", "item", "path")],
    abfss[c("workspace", "item", "path")]
  )

  expect_error(
    onelake_resolve_target(workspace_id, "Curated.Lakehouse"),
    "GUIDs to be used together",
    fixed = TRUE
  )
  expect_error(
    onelake_resolve_target("Analytics", "Curated"),
    "type suffix",
    fixed = TRUE
  )
  expect_error(
    onelake_resolve_target("https://example.test/ws/item/Files/x"),
    "not a Microsoft Fabric OneLake host",
    fixed = TRUE
  )
  regional <- onelake_resolve_target(
    "https://westeurope-api.onelake.fabric.microsoft.com/Analytics/Curated.Lakehouse/Files/x"
  )
  expect_equal(
    regional$dfs_base,
    "https://westeurope-api.onelake.fabric.microsoft.com"
  )
  regional_dfs <- onelake_resolve_target(
    "https://westeurope-onelake.dfs.fabric.microsoft.com/Analytics/Curated.Lakehouse/Files/x"
  )
  expect_equal(
    regional_dfs$dfs_base,
    "https://westeurope-onelake.dfs.fabric.microsoft.com"
  )
  private_workspace <- onelake_resolve_target(paste0(
    "https://",
    workspace_id,
    ".z12.dfs.fabric.microsoft.com/",
    workspace_id,
    "/",
    item_id,
    "/Files/x"
  ))
  expect_equal(
    private_workspace$dfs_base,
    paste0("https://", workspace_id, ".z12.dfs.fabric.microsoft.com")
  )
})

test_that("OneLake listing follows header continuation and preserves hierarchy", {
  calls <- list()
  pages <- list(
    onelake_test_response(
      body = list(
        paths = list(
          list(
            name = "Curated.Lakehouse/Files/a/duplicate.txt",
            isDirectory = FALSE,
            contentLength = "3",
            etag = "\"one\"",
            lastModified = "Fri, 24 Jul 2026 10:00:00 GMT"
          ),
          list(
            name = "Curated.Lakehouse/Files/b",
            isDirectory = "true",
            contentLength = "0"
          )
        )
      ),
      headers = list("x-ms-continuation" = "opaque+/= token")
    ),
    onelake_test_response(
      body = list(
        paths = list(
          list(
            name = "Curated.Lakehouse/Files/b/duplicate.txt",
            isDirectory = FALSE,
            contentLength = "4",
            etag = "\"two\""
          ),
          list(
            name = "Curated.Lakehouse/Files/unicode/café-数据.txt",
            isDirectory = FALSE,
            contentLength = "5"
          )
        )
      )
    )
  )
  httr2::local_mocked_responses(function(req) {
    calls[[length(calls) + 1L]] <<- req
    pages[[length(calls)]]
  })
  audiences <- character()

  files <- fabric_onelake_list(
    "Analytics",
    "Curated.Lakehouse",
    path = "Files",
    recursive = TRUE,
    page_size = 2L,
    token = function(audience, force_refresh = FALSE) {
      audiences <<- c(audiences, audience)
      "storage-token"
    }
  )

  expect_s3_class(files, "tbl_df")
  expect_equal(nrow(files), 4L)
  expect_equal(sum(files$name == "duplicate.txt"), 2L)
  expect_equal(
    files$path[files$name == "duplicate.txt"],
    c("Files/a/duplicate.txt", "Files/b/duplicate.txt")
  )
  expect_true(any(files$path == "Files/unicode/café-数据.txt"))
  expect_true(files$is_directory[files$path == "Files/b"])
  expect_equal(audiences, rep(.fabric_audience$storage, 2L))
  expect_match(calls[[1L]]$url, "recursive=true")
  expect_match(calls[[1L]]$url, "maxResults=2")
  expect_match(calls[[2L]]$url, "continuation=opaque%2B%2F%3D%20token")
})

test_that("OneLake metadata exposes properties and ETags", {
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    onelake_test_response(
      headers = list(
        "content-length" = "17",
        "content-type" = "text/plain",
        "etag" = "\"etag-value\"",
        "last-modified" = "Fri, 24 Jul 2026 10:00:00 GMT",
        "x-ms-resource-type" = "file",
        "x-ms-request-id" = "storage-request"
      ),
      url = req$url
    )
  })

  metadata <- fabric_onelake_metadata(
    "Analytics",
    "Curated.Lakehouse",
    "Files/café.txt",
    token = "token"
  )

  expect_equal(captured$method, "HEAD")
  expect_match(captured$url, "caf%C3%A9.txt")
  expect_equal(metadata$content_length, 17)
  expect_equal(metadata$content_type, "text/plain")
  expect_equal(metadata$etag, "\"etag-value\"")
  expect_false(metadata$is_directory)
  expect_equal(metadata$request_id, "storage-request")
})

test_that("OneLake download supports ranges, ETags, and atomic destinations", {
  captured <- list()
  httr2::local_mocked_responses(function(req) {
    captured[[length(captured) + 1L]] <<- req
    onelake_test_response(
      status = if (is.null(req$headers$Range)) 200L else 206L,
      body = charToRaw("alpha"),
      url = req$url
    )
  })

  value <- fabric_onelake_download(
    "Analytics",
    "Curated.Lakehouse",
    "Files/a.txt",
    range = c(1, 3),
    if_match = "\"etag\"",
    token = "token"
  )
  expect_identical(rawToChar(value), "alpha")
  expect_equal(captured[[1L]]$headers$Range, "bytes=1-3")
  expect_equal(captured[[1L]]$headers[["If-Match"]], "\"etag\"")
  expect_equal(onelake_if_match("0x8DA58EE365"), "\"0x8DA58EE365\"")

  dest <- tempfile("onelake-destination-")
  on.exit(unlink(dest), add = TRUE)
  local_mocked_bindings(
    .httr2_perform = function(req, download_path = NULL, ...) {
      writeBin(charToRaw("alpha"), download_path)
      onelake_test_response(body = charToRaw("alpha"), url = req$url)
    }
  )
  result <- fabric_onelake_download(
    "Analytics",
    "Curated.Lakehouse",
    "Files/a.txt",
    dest = dest,
    token = "token"
  )
  expect_true(file.exists(dest))
  expect_equal(readChar(dest, nchars = 5L, useBytes = TRUE), "alpha")
  expect_equal(result, normalizePath(dest, winslash = "/", mustWork = TRUE))
  expect_error(
    fabric_onelake_download(
      "Analytics",
      "Curated.Lakehouse",
      "Files/a.txt",
      dest = dest,
      token = "token"
    ),
    "Destination already exists",
    fixed = TRUE
  )
})

test_that("OneLake upload chunks to a temporary path and renames atomically", {
  captured <- list()
  httr2::local_mocked_responses(function(req) {
    captured[[length(captured) + 1L]] <<- req
    onelake_test_response(
      status = if (identical(req$method, "PUT")) 201L else 200L,
      headers = list(
        etag = "\"uploaded\"",
        "last-modified" = "Fri, 24 Jul 2026 10:00:00 GMT"
      ),
      url = req$url
    )
  })

  uploaded <- fabric_onelake_upload(
    "Analytics",
    "Curated.Lakehouse",
    "Files/file.txt",
    source = charToRaw("hello"),
    content_type = "text/plain; charset=utf-8",
    token = "token"
  )

  expect_equal(
    vapply(captured, function(req) req$method, character(1)),
    c("PUT", "PATCH", "PATCH", "PUT")
  )
  expect_match(captured[[1L]]$url, "resource=file")
  expect_match(captured[[1L]]$url, "fabricqueryr-upload")
  expect_equal(captured[[1L]]$headers[["If-None-Match"]], "*")
  expect_equal(
    captured[[1L]]$headers[["x-ms-content-type"]],
    "text/plain; charset=utf-8"
  )
  expect_match(captured[[2L]]$url, "action=append")
  expect_match(captured[[2L]]$url, "position=0")
  expect_identical(captured[[2L]]$body$data, charToRaw("hello"))
  expect_match(captured[[3L]]$url, "action=flush")
  expect_match(captured[[3L]]$url, "position=5")
  expect_match(captured[[4L]]$url, "Files/file.txt\\?mode=posix")
  expect_match(
    captured[[4L]]$headers[["x-ms-rename-source"]],
    "^/Analytics/Curated.Lakehouse/Files/\\.fabricqueryr-upload-"
  )
  expect_equal(captured[[4L]]$headers[["If-None-Match"]], "*")
  expect_equal(uploaded$content_length, 5)
  expect_equal(uploaded$etag, "\"uploaded\"")

  captured <- list()
  fabric_onelake_upload(
    "Analytics",
    "Curated.Lakehouse",
    "Files/file.txt",
    source = raw(),
    overwrite = TRUE,
    if_match = "\"old\"",
    token = "token"
  )
  expect_equal(length(captured), 3L)
  expect_equal(captured[[1L]]$headers[["If-None-Match"]], "*")
  expect_match(captured[[2L]]$url, "position=0")
  expect_equal(captured[[3L]]$headers[["If-Match"]], "\"old\"")
  expect_null(captured[[3L]]$headers[["If-None-Match"]])
})

test_that("OneLake upload streams local files in configured chunks", {
  captured <- list()
  httr2::local_mocked_responses(function(req) {
    captured[[length(captured) + 1L]] <<- req
    onelake_test_response(
      status = if (identical(req$method, "PUT")) 201L else 200L,
      url = req$url
    )
  })
  source <- tempfile("onelake-upload-")
  on.exit(unlink(source), add = TRUE)
  writeBin(charToRaw("abcdefgh"), source)

  fabric_onelake_upload(
    "Analytics",
    "Curated.Lakehouse",
    "Files/chunked.txt",
    source = source,
    chunk_size = 3,
    token = "token"
  )

  appends <- captured[vapply(
    captured,
    function(req) grepl("action=append", req$url, fixed = TRUE),
    logical(1)
  )]
  expect_length(appends, 3L)
  expect_match(appends[[1L]]$url, "position=0")
  expect_match(appends[[2L]]$url, "position=3")
  expect_match(appends[[3L]]$url, "position=6")
  expect_identical(
    lapply(appends, function(req) rawToChar(req$body$data)),
    list("abc", "def", "gh")
  )
  expect_match(captured[[5L]]$url, "position=8")
  expect_equal(captured[[6L]]$method, "PUT")
})

test_that("OneLake upload removes temporary files after transfer failure", {
  calls <- list()
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      calls[[length(calls) + 1L]] <<- req
      if (grepl("action=append", req$url, fixed = TRUE)) {
        rlang::abort("simulated append failure")
      }
      onelake_test_response(
        status = if (identical(req$method, "PUT")) 201L else 200L,
        url = req$url
      )
    }
  )

  expect_error(
    fabric_onelake_upload(
      "Analytics",
      "Curated.Lakehouse",
      "Files/failure.txt",
      source = charToRaw("content"),
      chunk_size = 3,
      token = "token"
    ),
    "simulated append failure",
    fixed = TRUE
  )

  expect_equal(
    vapply(calls, function(req) req$method, character(1)),
    c("PUT", "PATCH", "DELETE")
  )
  expect_match(calls[[3L]]$url, "fabricqueryr-upload")
  expect_false(any(grepl(
    "Files/failure.txt\\?mode=posix",
    vapply(
      calls,
      `[[`,
      character(1),
      "url"
    )
  )))
})

test_that("OneLake upload preserves conflict errors and creates nested parents", {
  calls <- list()
  httr2::local_mocked_responses(function(req) {
    calls[[length(calls) + 1L]] <<- req
    if (identical(req$method, "HEAD")) {
      return(onelake_test_response(404L, url = req$url))
    }
    if (
      identical(req$method, "PUT") &&
        !is.null(req$headers[["x-ms-rename-source"]])
    ) {
      return(onelake_test_response(
        412L,
        body = list(error = list(code = "PathAlreadyExists")),
        url = req$url
      ))
    }
    onelake_test_response(201L, url = req$url)
  })

  error <- expect_error(
    fabric_onelake_upload(
      "Analytics",
      "Curated.Lakehouse",
      "Files/nested/deeper/file.txt",
      source = charToRaw("content"),
      token = "token"
    ),
    "HTTP 412",
    fixed = TRUE
  )
  expect_match(conditionMessage(error), "PathAlreadyExists")
  expect_equal(
    vapply(calls[1:4], function(req) req$method, character(1)),
    c("HEAD", "PUT", "HEAD", "PUT")
  )
  expect_match(calls[[2L]]$url, "Files/nested\\?resource=directory")
  expect_match(calls[[4L]]$url, "Files/nested/deeper\\?resource=directory")
})

test_that("OneLake deletion is explicit, safe, conditional, and paginated", {
  expect_error(
    fabric_onelake_delete(
      "Analytics",
      "Curated.Lakehouse",
      "Files/folder",
      token = "token"
    ),
    "disabled by default",
    fixed = TRUE
  )
  expect_error(
    fabric_onelake_delete(
      "Analytics",
      "Curated.Lakehouse",
      "Files",
      confirm = TRUE,
      token = "token"
    ),
    "Fabric-managed first-level folder",
    fixed = TRUE
  )

  calls <- list()
  httr2::local_mocked_responses(function(req) {
    calls[[length(calls) + 1L]] <<- req
    onelake_test_response(
      status = 200L,
      headers = if (length(calls) == 1L) {
        list("x-ms-continuation" = "delete-token")
      } else {
        list()
      },
      url = req$url
    )
  })
  expect_true(fabric_onelake_delete(
    "Analytics",
    "Curated.Lakehouse",
    "Files/folder",
    recursive = TRUE,
    confirm = TRUE,
    if_match = "\"etag\"",
    token = "token"
  ))
  expect_equal(length(calls), 2L)
  expect_true(all(
    vapply(calls, function(req) req$method, character(1)) == "DELETE"
  ))
  expect_match(calls[[1L]]$url, "recursive=true")
  expect_match(calls[[1L]]$url, "paginated=true")
  expect_match(calls[[2L]]$url, "continuation=delete-token")
  expect_equal(calls[[1L]]$headers[["If-Match"]], "\"etag\"")
})

test_that("OneLake validates ranges and protected paths before I/O", {
  expect_error(onelake_validate_range(c(-1, 2)), "non-negative")
  expect_error(onelake_validate_range(c(3, 2)), "non-negative")
  target <- onelake_resolve_target(
    "Analytics",
    "Curated.Lakehouse",
    "Files"
  )
  credential <- fabric_credential(token = "token")
  for (page_size in list(0, 5001, 1.5, NA_real_, Inf, "10", c(1, 2))) {
    expect_error(
      onelake_list_target(target, credential, page_size = page_size),
      "page_size must be one whole number between 1 and 5000",
      fixed = TRUE
    )
  }
  expect_error(
    fabric_onelake_upload(
      "Analytics",
      "Curated.Lakehouse",
      "Files",
      source = raw(),
      token = "token"
    ),
    "Fabric-managed first-level folder",
    fixed = TRUE
  )
  expect_error(
    onelake_resolve_target(
      "Analytics",
      "Curated.Lakehouse",
      "Files/../Tables/data"
    ),
    "unsafe segment",
    fixed = TRUE
  )
})
