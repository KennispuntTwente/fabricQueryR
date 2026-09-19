test_that("R6 job lifecycle requests preserve execution credentials", {
  discovery <- fabric_credential(token = "discovery")
  execution <- fabric_credential(token = "execution")
  override <- fabric_credential(token = "override")
  item <- fabric_r6_record(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      type = "Notebook"
    ),
    c("fabric_item", "list"),
    discovery
  )
  id <- "33333333-3333-4333-8333-333333333333"
  job <- .fabric_job_context(
    id,
    item = item,
    token = execution,
    use_workspace_endpoint = FALSE
  )$job
  used <- NULL
  local_mocked_bindings(.fabric_job_request = function(
    method,
    url,
    credential,
    ...
  ) {
    used <<- credential
    list(
      status_code = if (method == "GET") 200L else 202L,
      body = list(id = id, status = "Completed")
    )
  })
  instance <- fabric_job_status(job)
  for (handle in list(job, instance)) {
    for (method in c("status", "wait", "cancel")) {
      item[[method]](handle)
      expect_identical(used, execution)
      item[[method]](handle, token = override)
      expect_identical(used, override)
      item[[method]](unserialize(serialize(handle, NULL)))
      expect_identical(used, discovery)
    }
  }
  for (method in c("status", "cancel")) {
    item[[method]](id)
    expect_identical(used, discovery)
  }
})

test_that("R6 refresh lifecycle requests preserve execution credentials", {
  discovery <- fabric_credential(token = "discovery")
  execution <- fabric_credential(token = "execution")
  override <- fabric_credential(token = "override")
  model <- fabric_r6_record(
    list(
      id = "11111111-1111-4111-8111-111111111111",
      workspaceId = "22222222-2222-4222-8222-222222222222",
      type = "SemanticModel"
    ),
    c("fabric_item", "list"),
    discovery
  )
  id <- "33333333-3333-4333-8333-333333333333"
  refresh <- .pbi_refresh_handle(
    id,
    list(
      workspace_id = model$workspaceId,
      dataset_id = model$id,
      my_workspace = FALSE
    ),
    execution,
    "https://api.powerbi.com/v1.0/myorg",
    mode = "enhanced"
  )
  used <- NULL
  local_mocked_bindings(.pbi_refresh_request = function(
    method,
    url,
    credential,
    ...
  ) {
    used <<- credential
    body <- if (grepl("/refreshes$", url)) {
      list(
        value = list(list(
          requestId = id,
          refreshType = "ViaEnhancedApi",
          status = "Completed"
        ))
      )
    } else {
      list(requestId = id, status = "Completed", extendedStatus = "Completed")
    }
    list(status_code = if (method == "GET") 200L else 202L, body = body)
  })
  detail <- fabric_pbi_refresh_status(refresh)
  for (handle in list(refresh, detail)) {
    for (method in c("refresh_status", "refresh_wait", "refresh_cancel")) {
      model[[method]](handle)
      expect_identical(used, execution)
      model[[method]](handle, token = override)
      expect_identical(used, override)
      model[[method]](unserialize(serialize(handle, NULL)))
      expect_identical(used, discovery)
    }
  }
  for (method in c("refresh_status", "refresh_cancel")) {
    model[[method]](id)
    expect_identical(used, discovery)
  }
})
