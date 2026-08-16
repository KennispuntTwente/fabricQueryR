test_that("OneLake object writer serializes supported Arrow formats", {
  skip_if_not_installed("arrow")
  data <- data.frame(id = 1:3, label = c("a", "b", NA))
  captured <- list()
  local_mocked_bindings(
    fabric_onelake_upload = function(path, source, content_type, ...) {
      value <- switch(
        tools::file_ext(path),
        parquet = as.data.frame(arrow::read_parquet(source)),
        csv = as.data.frame(arrow::read_csv_arrow(source)),
        arrow = as.data.frame(arrow::read_ipc_stream(source))
      )
      captured[[path]] <<- list(
        value = value,
        content_type = content_type,
        bytes = file.info(source)$size
      )
      tibble::tibble(
        path = path,
        name = basename(path),
        is_directory = FALSE,
        content_length = file.info(source)$size,
        content_type = content_type,
        etag = '"etag"',
        last_modified = NA_character_,
        content_range = NA_character_,
        request_id = "request-id"
      )
    }
  )

  for (format in c("parquet", "csv", "arrow")) {
    path <- paste0("Files/data.", format)
    result <- fabric_onelake_write_file(
      "workspace",
      "lakehouse.Lakehouse",
      path,
      data,
      token = "token"
    )
    expect_s3_class(result, "fabric_onelake_file_write_result")
    expect_identical(result$format, format)
    expect_identical(result$columns[[1L]], c("id", "label"))
    expect_gt(captured[[path]]$bytes, 0)
    expect_equal(captured[[path]]$value$id, 1:3)
    expect_equal(captured[[path]]$value$label, c("a", "b", NA))
  }
  expect_equal(
    captured[["Files/data.parquet"]]$content_type,
    "application/vnd.apache.parquet"
  )
  expect_equal(
    captured[["Files/data.csv"]]$content_type,
    "text/csv; charset=utf-8"
  )
  expect_equal(
    captured[["Files/data.arrow"]]$content_type,
    "application/vnd.apache.arrow.stream"
  )
})

test_that("OneLake object writer consumes lazy Arrow streams", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  uploaded <- NULL
  reader <- arrow::as_record_batch_reader(data.frame(id = 1:4))
  stream <- nanoarrow::as_nanoarrow_array_stream(reader)
  local_mocked_bindings(
    fabric_onelake_upload = function(source, ...) {
      uploaded <<- as.data.frame(arrow::read_parquet(source))
      tibble::tibble(path = "Files/stream.parquet")
    }
  )

  result <- fabric_onelake_write_file(
    "workspace",
    "lakehouse.Lakehouse",
    "Files/stream.parquet",
    stream,
    token = "token"
  )

  expect_equal(uploaded$id, 1:4)
  expect_equal(result$rows, 4)
})

test_that("OneLake object reader returns tibbles and lazy streams", {
  skip_if_not_installed("arrow")
  skip_if_not_installed("nanoarrow")
  data <- data.frame(id = 1:3, label = c("a", "b", "c"))
  fixtures <- list()
  fixtures$parquet <- tempfile(fileext = ".parquet")
  fixtures$csv <- tempfile(fileext = ".csv")
  fixtures$arrow <- tempfile(fileext = ".arrow")
  on.exit(unlink(unlist(fixtures), force = TRUE), add = TRUE)
  arrow::write_parquet(data, fixtures$parquet)
  arrow::write_csv_arrow(data, fixtures$csv)
  arrow::write_ipc_stream(data, fixtures$arrow)
  local_mocked_bindings(
    fabric_onelake_download = function(path, dest, ...) {
      extension <- tools::file_ext(path)
      file.copy(fixtures[[extension]], dest)
      invisible(dest)
    }
  )

  for (format in names(fixtures)) {
    value <- fabric_onelake_read_file(
      "workspace",
      "lakehouse.Lakehouse",
      paste0("Files/data.", format),
      token = "token"
    )
    expect_s3_class(value, "tbl_df")
    expect_equal(value$id, 1:3)
    expect_equal(value$label, c("a", "b", "c"))
  }

  for (format in names(fixtures)) {
    stream <- fabric_onelake_read_file(
      "workspace",
      "lakehouse.Lakehouse",
      paste0("Files/data.", format),
      result = "arrow_stream",
      token = "token"
    )
    local_path <- attr(stream, "fabric_onelake_file_path", exact = TRUE)
    on.exit(unlink(local_path, force = TRUE), add = TRUE)
    expect_s3_class(stream, "nanoarrow_array_stream")
    expect_true(file.exists(local_path))
    streamed <- as.data.frame(
      arrow::as_record_batch_reader(stream)$read_table()
    )
    expect_equal(streamed, data)
  }
})

test_that("OneLake object wrappers retain discovered DFS endpoints", {
  skip_if_not_installed("arrow")
  workspace_id <- "11111111-1111-1111-1111-111111111111"
  item_id <- "22222222-2222-2222-2222-222222222222"
  private_dfs <- paste0(
    "https://",
    workspace_id,
    ".z12.dfs.fabric.microsoft.com"
  )
  workspace <- list(
    id = workspace_id,
    oneLakeEndpoints = list(dfsEndpoint = private_dfs)
  )
  item <- list(
    id = item_id,
    workspaceId = workspace_id,
    type = "Lakehouse"
  )
  fixture <- tempfile(fileext = ".parquet")
  on.exit(unlink(fixture, force = TRUE), add = TRUE)
  arrow::write_parquet(data.frame(id = 1L), fixture)
  endpoints <- list()
  local_mocked_bindings(
    fabric_onelake_download = function(
      workspace,
      item,
      path,
      dest,
      item_type,
      dfs_base,
      ...
    ) {
      target <- onelake_resolve_target(
        workspace,
        item,
        path,
        item_type,
        dfs_base
      )
      endpoints$read <<- target$dfs_base
      file.copy(fixture, dest)
      invisible(dest)
    },
    fabric_onelake_upload = function(
      workspace,
      item,
      path,
      source,
      item_type,
      dfs_base,
      ...
    ) {
      target <- onelake_resolve_target(
        workspace,
        item,
        path,
        item_type,
        dfs_base
      )
      endpoints$write <<- target$dfs_base
      tibble::tibble(path = path)
    }
  )

  read <- fabric_onelake_read_file(
    workspace,
    item,
    "Files/input.parquet",
    token = "token"
  )
  write <- fabric_onelake_write_file(
    workspace,
    item,
    "Files/output.parquet",
    data.frame(id = 1L),
    token = "token"
  )

  expect_equal(read$id, 1L)
  expect_equal(write$path, "Files/output.parquet")
  expect_identical(endpoints$read, private_dfs)
  expect_identical(endpoints$write, private_dfs)
})

test_that("OneLake object file formats are explicit and validated", {
  expect_identical(
    .fabric_onelake_object_format("Files/data.PQ", "auto"),
    "parquet"
  )
  expect_identical(
    .fabric_onelake_object_format("Files/no-extension", "csv"),
    "csv"
  )
  expect_error(
    .fabric_onelake_object_format("Files/data.json", "auto"),
    "Could not infer format",
    class = "fabric_onelake_object_format_error"
  )
  expect_error(
    fabric_onelake_write_file(
      "workspace",
      "lakehouse.Lakehouse",
      "Files/data.csv",
      data.frame(id = 1L),
      include_header = NA,
      token = "token"
    ),
    "include_header must be TRUE or FALSE"
  )
})

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
  reserved <- onelake_resolve_target(
    "Analytics",
    "Curated.Lakehouse",
    "Files/a?b#c.csv"
  )
  expect_match(onelake_path_url(reserved), "Files/a%3Fb%23c.csv", fixed = TRUE)
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

  complete_reserved <- onelake_resolve_target(paste0(
    "https://onelake.dfs.fabric.microsoft.com/",
    workspace_id,
    "/",
    item_id,
    "/Files/a%3Fb%23c.csv"
  ))
  expect_equal(complete_reserved$path, "Files/a?b#c.csv")
  expect_match(
    onelake_path_url(complete_reserved),
    "Files/a%3Fb%23c.csv",
    fixed = TRUE
  )
  expect_error(
    onelake_resolve_target(paste0(
      "https://user@onelake.dfs.fabric.microsoft.com/",
      workspace_id,
      "/",
      item_id
    )),
    "must not include user information",
    fixed = TRUE
  )
  expect_error(
    onelake_resolve_target(paste0(
      "https://onelake.dfs.fabric.microsoft.com:444/",
      workspace_id,
      "/",
      item_id
    )),
    "default port",
    fixed = TRUE
  )
  expect_equal(discovered$item, item_id)

  private_dfs <- paste0(
    "https://",
    gsub("-", "", workspace_id),
    ".z12.dfs.fabric.microsoft.com"
  )
  discovered_endpoint <- onelake_resolve_target(
    list(
      id = workspace_id,
      oneLakeEndpoints = list(dfsEndpoint = private_dfs)
    ),
    list(id = item_id, workspaceId = workspace_id, type = "Lakehouse"),
    "Files/nested/file.csv"
  )
  expect_equal(discovered_endpoint$dfs_base, private_dfs)
  expect_equal(
    onelake_resolve_target(
      list(
        id = workspace_id,
        oneLakeEndpoints = list(dfsEndpoint = private_dfs)
      ),
      list(id = item_id, workspaceId = workspace_id, type = "Lakehouse"),
      dfs_base = "https://westeurope-onelake.dfs.fabric.microsoft.com"
    )$dfs_base,
    "https://westeurope-onelake.dfs.fabric.microsoft.com"
  )

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
    onelake_resolve_target("Analytics/Other", "Curated.Lakehouse"),
    "workspace must be exactly one URI path segment",
    fixed = TRUE
  )
  for (unsafe_workspace in c(".", "..", "%2e%2e", "%252e%252e")) {
    expect_error(
      onelake_resolve_target(unsafe_workspace, "Curated.Lakehouse"),
      "workspace must be exactly one URI path segment",
      fixed = TRUE
    )
  }
  for (unsafe_item in c(".", "..", "%2f", "%255c")) {
    expect_error(
      onelake_resolve_target(
        "Analytics",
        unsafe_item,
        item_type = "Lakehouse"
      ),
      "item must be exactly one URI path segment",
      fixed = TRUE
    )
  }
  expect_error(
    onelake_resolve_target(
      "Analytics",
      "Folder/Curated",
      item_type = "Lakehouse"
    ),
    "item must be exactly one URI path segment",
    fixed = TRUE
  )
  expect_error(
    onelake_resolve_target(
      "Analytics",
      "Curated.Warehouse",
      item_type = "Lakehouse"
    ),
    "conflicts with the item's existing type suffix",
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

  compact_workspace <- gsub("-", "", workspace_id, fixed = TRUE)
  workspace_dfs <- paste0(
    "https://",
    compact_workspace,
    ".z12.dfs.fabric.microsoft.com"
  )
  item_scoped <- onelake_resolve_target(paste0(
    workspace_dfs,
    "/",
    item_id,
    "/Ingestion/Queue"
  ))
  expect_equal(item_scoped$workspace, workspace_id)
  expect_equal(item_scoped$item, item_id)
  expect_equal(item_scoped$path, "Ingestion/Queue")
  expect_equal(item_scoped$dfs_base, workspace_dfs)

  blob_scoped <- onelake_resolve_target(paste0(
    "https://",
    compact_workspace,
    ".z12.blob.fabric.microsoft.com/",
    item_id,
    "/Files/x"
  ))
  expect_equal(blob_scoped$workspace, workspace_id)
  expect_equal(blob_scoped$dfs_base, workspace_dfs)
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

test_that("OneLake listing rejects repeated continuation tokens", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    onelake_test_response(
      body = list(paths = list()),
      headers = list("x-ms-continuation" = "repeated-token")
    )
  })

  expect_error(
    fabric_onelake_list(
      "Analytics",
      "Curated.Lakehouse",
      path = "Files",
      token = "token"
    ),
    "repeated pagination URL",
    fixed = TRUE
  )
  expect_equal(calls, 2L)
})

test_that("OneLake listing can begin from a lexicographic path", {
  captured <- NULL
  httr2::local_mocked_responses(function(req) {
    captured <<- req
    onelake_test_response(body = list(paths = list()))
  })
  fabric_onelake_list(
    "Analytics",
    "Curated.Lakehouse",
    path = "Tables/table/_delta_log",
    token = "token",
    begin_from = "00000000000000000100"
  )

  expect_match(captured$url, "beginFrom=00000000000000000100")
  expect_error(
    fabric_onelake_list(
      "Analytics",
      "Curated.Lakehouse",
      path = "Tables/table/_delta_log",
      token = "token",
      begin_from = "../outside"
    ),
    "unsafe segment",
    fixed = TRUE
  )
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

test_that("OneLake download supports ranges, ETags, and staged destinations", {
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
  expect_equal(
    fabric_onelake_download(
      "Analytics",
      "Curated.Lakehouse",
      "Files/a.txt",
      dest = dest,
      overwrite = TRUE,
      token = "token"
    ),
    normalizePath(dest, winslash = "/", mustWork = TRUE)
  )
  expect_equal(readChar(dest, nchars = 5L, useBytes = TRUE), "alpha")
})

test_that("failed download replacement restores the original destination", {
  dest <- tempfile("onelake-existing-")
  on.exit(unlink(dest), add = TRUE)
  writeBin(charToRaw("original"), dest)
  rename_calls <- 0L
  local_mocked_bindings(
    .httr2_perform = function(req, download_path = NULL, ...) {
      writeBin(charToRaw("replacement"), download_path)
      onelake_test_response(body = charToRaw("replacement"), url = req$url)
    },
    .onelake_file_rename = function(from, to) {
      rename_calls <<- rename_calls + 1L
      if (rename_calls == 2L) {
        return(FALSE)
      }
      file.rename(from, to)
    }
  )

  expect_error(
    fabric_onelake_download(
      "Analytics",
      "Curated.Lakehouse",
      "Files/a.txt",
      dest = dest,
      overwrite = TRUE,
      token = "token"
    ),
    "original destination was restored",
    fixed = TRUE
  )
  expect_equal(
    readChar(dest, nchars = 8L, useBytes = TRUE),
    "original"
  )
  expect_equal(rename_calls, 3L)
})

test_that("OneLake download never replaces a directory destination", {
  dest <- tempfile("onelake-directory-")
  dir.create(dest)
  sentinel <- file.path(dest, "sentinel.txt")
  writeLines("keep", sentinel)
  on.exit(unlink(dest, recursive = TRUE, force = TRUE), add = TRUE)
  performed <- FALSE
  local_mocked_bindings(
    .httr2_perform = function(...) {
      performed <<- TRUE
      rlang::abort("request should not be performed")
    }
  )

  expect_error(
    fabric_onelake_download(
      "Analytics",
      "Curated.Lakehouse",
      "Files/a.txt",
      dest = dest,
      overwrite = TRUE,
      token = "token"
    ),
    "Destination is a directory",
    fixed = TRUE
  )
  expect_false(performed)
  expect_equal(readLines(sentinel), "keep")
})

test_that("download commit rechecks no-overwrite destinations", {
  temporary <- tempfile("onelake-staged-")
  dest <- tempfile("onelake-raced-")
  on.exit(unlink(c(temporary, dest), force = TRUE), add = TRUE)
  writeBin(charToRaw("replacement"), temporary)
  writeBin(charToRaw("winner"), dest)

  expect_error(
    onelake_commit_download(temporary, dest, overwrite = FALSE),
    "Destination already exists",
    fixed = TRUE
  )
  expect_equal(readChar(dest, nchars = 6L, useBytes = TRUE), "winner")
  expect_true(file.exists(temporary))
})

test_that("no-overwrite download commit preserves one race winner", {
  temporary <- tempfile("onelake-staged-")
  dest <- tempfile("onelake-linked-")
  on.exit(unlink(c(temporary, dest), force = TRUE), add = TRUE)
  writeBin(charToRaw("download"), temporary)
  local_mocked_bindings(
    .onelake_file_link = function(from, to) {
      writeBin(charToRaw("winner"), to)
      FALSE
    }
  )

  expect_error(
    onelake_commit_new_download(temporary, dest),
    "Destination already exists",
    fixed = TRUE
  )
  expect_equal(readChar(dest, nchars = 6L, useBytes = TRUE), "winner")
  expect_true(file.exists(temporary))
})

test_that("OneLake download returns raw zero bytes for an empty file", {
  httr2::local_mocked_responses(function(req) {
    onelake_test_response(
      body = raw(),
      headers = list(`content-length` = "0"),
      url = req$url
    )
  })

  value <- fabric_onelake_download(
    "Analytics",
    "Curated.Lakehouse",
    "Files/empty.bin",
    token = "token"
  )

  expect_identical(value, raw())
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
  expect_equal(
    captured[[3L]]$headers[["x-ms-content-type"]],
    "text/plain; charset=utf-8"
  )
  expect_match(captured[[4L]]$url, "Files/file.txt\\?mode=posix")
  expect_match(
    captured[[4L]]$headers[["x-ms-rename-source"]],
    "^/Analytics/Curated.Lakehouse/Files/\\.fabricqueryr-upload-"
  )
  expect_equal(
    captured[[4L]]$headers[["x-ms-content-type"]],
    "text/plain; charset=utf-8"
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
  expect_false(grepl("recursive=", calls[[3L]]$url, fixed = TRUE))
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

test_that("OneLake upload cleans up an ambiguously failed temporary create", {
  calls <- list()
  local_mocked_bindings(
    .httr2_perform = function(req, ...) {
      calls[[length(calls) + 1L]] <<- req
      if (length(calls) == 1L) {
        rlang::abort("connection closed after the create was sent")
      }
      onelake_test_response(status = 200L, url = req$url)
    }
  )

  expect_error(
    fabric_onelake_upload(
      "Analytics",
      "Curated.Lakehouse",
      "Files/create-failure.txt",
      source = charToRaw("content"),
      token = "token"
    ),
    "connection closed after the create was sent",
    fixed = TRUE
  )

  expect_equal(
    vapply(calls, function(req) req$method, character(1)),
    c("PUT", "DELETE")
  )
  expect_match(calls[[1L]]$url, "fabricqueryr-upload", fixed = TRUE)
  expect_match(calls[[2L]]$url, "fabricqueryr-upload", fixed = TRUE)
  expect_equal(
    sub("\\?.*$", "", calls[[1L]]$url),
    sub("\\?.*$", "", calls[[2L]]$url)
  )
  expect_false(grepl("recursive=", calls[[2L]]$url, fixed = TRUE))
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
    if (identical(req$method, "HEAD")) {
      return(onelake_test_response(
        headers = list("x-ms-resource-type" = "directory"),
        url = req$url
      ))
    }
    onelake_test_response(
      status = 200L,
      headers = if (length(calls) == 2L) {
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
  expect_equal(length(calls), 3L)
  expect_equal(calls[[1L]]$method, "HEAD")
  expect_true(all(
    vapply(calls[-1L], function(req) req$method, character(1)) == "DELETE"
  ))
  expect_match(calls[[2L]]$url, "recursive=true")
  expect_match(calls[[2L]]$url, "paginated=true")
  expect_match(calls[[3L]]$url, "continuation=delete-token")
  expect_equal(calls[[2L]]$headers[["If-Match"]], "\"etag\"")
})

test_that("OneLake deletion omits directory parameters for files", {
  calls <- list()
  httr2::local_mocked_responses(function(req) {
    calls[[length(calls) + 1L]] <<- req
    onelake_test_response(
      headers = if (identical(req$method, "HEAD")) {
        list("x-ms-resource-type" = "file")
      } else {
        list()
      },
      url = req$url
    )
  })

  expect_true(fabric_onelake_delete(
    "Analytics",
    "Curated.Lakehouse",
    "Files/file.txt",
    recursive = TRUE,
    confirm = TRUE,
    token = "token"
  ))
  expect_equal(
    vapply(calls, function(req) req$method, character(1)),
    c("HEAD", "DELETE")
  )
  expect_false(grepl("recursive=", calls[[2L]]$url, fixed = TRUE))
  expect_false(grepl("paginated=", calls[[2L]]$url, fixed = TRUE))
})

test_that("OneLake deletion reconciles an ambiguous retry with 404", {
  accepted <- NULL
  local_mocked_bindings(
    .httr2_perform = function(
      req,
      ...,
      accepted_status = integer()
    ) {
      accepted <<- accepted_status
      onelake_test_response(status = 404L, url = req$url)
    }
  )
  target <- onelake_resolve_target(
    "Analytics",
    "Curated.Lakehouse",
    "Files/already-deleted.txt"
  )

  expect_true(onelake_delete_target(
    target,
    fabric_credential(token = "token"),
    is_directory = FALSE
  ))
  expect_identical(accepted, 404L)
})

test_that("OneLake deletion rejects repeated continuation tokens", {
  calls <- 0L
  httr2::local_mocked_responses(function(req) {
    calls <<- calls + 1L
    if (identical(req$method, "HEAD")) {
      return(onelake_test_response(
        headers = list("x-ms-resource-type" = "directory"),
        url = req$url
      ))
    }
    onelake_test_response(
      status = 200L,
      headers = list("x-ms-continuation" = "repeated-token"),
      url = req$url
    )
  })

  expect_error(
    fabric_onelake_delete(
      "Analytics",
      "Curated.Lakehouse",
      "Files/folder",
      recursive = TRUE,
      confirm = TRUE,
      token = "token"
    ),
    "repeated pagination URL",
    fixed = TRUE
  )
  expect_equal(calls, 3L)
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

test_that("OneLake blocks managed Delta file mutations by default", {
  upload_calls <- 0L
  local_mocked_bindings(
    onelake_upload_target = function(...) {
      upload_calls <<- upload_calls + 1L
      invisible(TRUE)
    }
  )
  protected <- c(
    "Tables/orders/_delta_log/00000000000000000001.json",
    "Tables/orders/part-00001.parquet",
    "Tables/sales/orders/_delta_log/00000000000000000001.json",
    "tables/sales/orders/part-00001.parquet"
  )
  for (path in protected) {
    expect_error(
      fabric_onelake_upload(
        "Analytics",
        "Curated.Lakehouse",
        path,
        source = raw(),
        token = "token"
      ),
      "below Tables/ is blocked",
      fixed = TRUE
    )
    expect_error(
      fabric_onelake_delete(
        "Analytics",
        "Curated.Lakehouse",
        path,
        confirm = TRUE,
        token = "token"
      ),
      "below Tables/ is blocked",
      fixed = TRUE
    )
  }
  expect_identical(upload_calls, 0L)
})

test_that("OneLake managed-table mutations require a dangerous opt-in", {
  target <- onelake_resolve_target(
    "Analytics",
    "Curated.Lakehouse",
    "Tables/orders/part-00001.parquet"
  )
  expect_no_error(onelake_require_mutable_path(
    target,
    "upload",
    allow_managed_tables = TRUE
  ))
  expect_error(
    onelake_require_mutable_path(
      target,
      "upload",
      allow_managed_tables = NA
    ),
    "must be TRUE or FALSE",
    fixed = TRUE
  )
})

test_that("OneLake DFS bases must be canonical HTTPS origins", {
  resolve <- function(dfs_base) {
    onelake_resolve_target(
      "Analytics",
      "Curated.Lakehouse",
      dfs_base = dfs_base
    )
  }
  host <- "onelake.dfs.fabric.microsoft.com"

  expect_error(
    resolve(paste0("https://user:secret@", host)),
    "must not include user information",
    fixed = TRUE
  )
  expect_error(
    resolve(paste0("https://", host, ":444")),
    "default HTTPS port",
    fixed = TRUE
  )
  expect_error(
    resolve(paste0("https://", host, "?x=y")),
    "query string or fragment",
    fixed = TRUE
  )
  expect_error(
    resolve(paste0("https://", host, "#fragment")),
    "query string or fragment",
    fixed = TRUE
  )

  target <- resolve(paste0("https://", host, ":443"))
  expect_equal(target$dfs_base, paste0("https://", host, ":443"))
})
