r6_test_record <- function(type = "Lakehouse", credential = NULL) {
  workspace <- identical(type, "Workspace")
  record <- list(
    id = if (workspace) {
      "22222222-2222-4222-8222-222222222222"
    } else {
      "11111111-1111-4111-8111-111111111111"
    },
    displayName = type,
    type = type
  )
  if (!workspace) {
    record$workspaceId <- "22222222-2222-4222-8222-222222222222"
  }
  fabric_r6_record(
    record,
    legacy_class = if (workspace) {
      c("fabric_workspace", "list")
    } else {
      c("fabric_item", "list")
    },
    credential = credential
  )
}

local_r6_method_mocks <- function(function_names, calls, env = parent.frame()) {
  mocks <- lapply(function_names, function(function_name) {
    force(function_name)
    function(...) {
      calls[[function_name]] <- list(...)
      function_name
    }
  })
  names(mocks) <- function_names
  mocks$.package <- "fabricQueryR"
  mocks$.env <- env
  do.call(testthat::local_mocked_bindings, mocks)
  invisible(NULL)
}

expect_r6_delegation <- function(
  object,
  method,
  args,
  function_name,
  calls,
  context_name = NULL
) {
  result <- do.call(object[[method]], args)
  expect_identical(result, function_name, info = method)
  call <- calls[[function_name]]
  expect_identical(typeof(call), "list", info = method)
  if (!is.null(context_name)) {
    expect_identical(call[[context_name]], object, info = method)
  }
  call
}
