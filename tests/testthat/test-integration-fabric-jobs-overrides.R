# Fabric integration coverage: on-demand execution and compute overrides
test_that("on-demand Spark overrides execute the requested file and arguments", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  spark <- fabric_test_manifest_item(manifest, "TestSparkJob")
  environment <- fabric_item(
    manifest$workspace_id,
    "TestEnvironment",
    type = "Environment",
    token = token
  )
  item <- fabric_item(manifest$workspace_id, spark$id, token = token)
  folder <- paste0(
    "Files/fabricqueryr_job_override_",
    .fabric_lakehouse_staging_id()
  )
  marker <- basename(folder)
  uri <- paste0(
    "abfss://",
    manifest$workspace_id,
    "@onelake.dfs.fabric.microsoft.com/",
    lakehouse$id,
    "/",
    folder
  )
  on.exit(
    try(
      fabric_onelake_delete(
        manifest$workspace_id,
        lakehouse$id,
        folder,
        recursive = TRUE,
        confirm = TRUE,
        token = token
      ),
      silent = TRUE
    ),
    add = TRUE
  )
  script <- paste(
    "import sys",
    "from pyspark.sql import SparkSession",
    "spark = SparkSession.builder.getOrCreate()",
    "assert len(sys.argv) == 3, sys.argv",
    "spark.createDataFrame([(sys.argv[1], 17)], 'marker string, value int').coalesce(1).write.mode('overwrite').json(sys.argv[2])",
    sep = "\n"
  )
  fabric_onelake_upload(
    manifest$workspace_id,
    lakehouse$id,
    paste0(folder, "/override.py"),
    source = charToRaw(script),
    token = token
  )
  reference <- function(id) {
    list(
      referenceType = "ById",
      itemId = id,
      workspaceId = manifest$workspace_id
    )
  }
  job <- fabric_job_run(
    item,
    execution_data = list(
      executableFile = paste0(uri, "/override.py"),
      commandLineArguments = paste(marker, paste0(uri, "/result")),
      defaultLakehouseId = reference(lakehouse$id),
      environmentId = reference(environment$id)
    ),
    token = token
  )
  on.exit(try(fabric_job_cancel(job), silent = TRUE), add = TRUE)
  completed <- fabric_job_wait(job, timeout = 1200, cancel_on_timeout = TRUE)
  expect_identical(completed$status, "Completed")
  files <- fabric_onelake_list(
    manifest$workspace_id,
    lakehouse$id,
    paste0(folder, "/result"),
    token = token
  )
  parts <- files$path[grepl("/part-.*[.]json$", files$path)]
  expect_length(parts, 1L)
  observed <- jsonlite::fromJSON(rawToChar(fabric_onelake_download(
    manifest$workspace_id,
    lakehouse$id,
    parts[[1L]],
    token = token
  )))
  expect_identical(observed$marker, marker)
  expect_identical(observed$value, 17L)
})

test_that("Notebook runs apply an attached Environment and Spark properties", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  notebook <- fabric_test_manifest_item(manifest, "JobFixtures")
  environment <- fabric_item(
    manifest$workspace_id,
    "TestEnvironment",
    type = "Environment",
    token = token
  )
  lakehouse <- fabric_test_manifest_item(manifest, "TestLakehouse")
  reference <- function(id) {
    list(
      referenceType = "ById",
      itemId = id,
      workspaceId = manifest$workspace_id
    )
  }
  marker <- paste0("environment_", .fabric_lakehouse_staging_id())
  job <- fabric_job_run(
    notebook$id,
    workspace = manifest$workspace_id,
    item_type = "Notebook",
    parameters = list(mode = "configuration", marker = marker),
    execution_data = list(
      compute = "Spark",
      computeConfiguration = list(
        defaultLakehouse = reference(lakehouse$id),
        attachedEnvironment = reference(environment$id),
        sparkProperties = list(list(
          key = "spark.sql.shuffle.partitions",
          value = "2"
        ))
      )
    ),
    token = token
  )
  on.exit(try(fabric_job_cancel(job), silent = TRUE), add = TRUE)
  completed <- fabric_job_wait(
    job,
    timeout = 1200,
    cancel_on_timeout = TRUE,
    notebook_details = TRUE
  )
  expect_identical(completed$status, "Completed")
  observed <- jsonlite::fromJSON(completed$exit_value)
  expect_identical(observed$marker, marker)
  expect_identical(observed$shuffle_partitions, "2")
  expect_identical(observed$environment_broadcast_timeout, "301")
  expect_identical(observed$lakehouse_id, lakehouse$id)
  expect_identical(observed$row_count, 3L)
})

test_that("Python notebook code runs with Jupyter compute", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  credential <- fabric_credential(token = token)
  name <- paste0("fabricqueryr_jupyter_", .fabric_lakehouse_staging_id())
  base <- paste0(.fabric_api_base, "/workspaces/", manifest$workspace_id)
  cell <- function(source, parameter = FALSE) {
    list(
      cell_type = "code",
      source = source,
      execution_count = NULL,
      outputs = list(),
      metadata = list(
        microsoft = list(
          language = "python",
          language_group = "jupyter_python"
        ),
        tags = if (parameter) list("parameters") else list()
      )
    )
  }
  content <- jsonlite::toJSON(
    list(
      nbformat = 4L,
      nbformat_minor = 5L,
      metadata = list(
        kernel_info = list(
          name = "jupyter",
          jupyter_kernel_name = "python3.11"
        ),
        language_info = list(name = "python")
      ),
      cells = list(
        cell('marker = "default"', TRUE),
        cell(paste(
          "import notebookutils",
          'notebookutils.notebook.exit(f"fabricqueryr-job-success:{marker}")',
          sep = "\n"
        ))
      )
    ),
    auto_unbox = TRUE,
    null = "null"
  )
  request <- httr2::request(paste0(base, "/notebooks")) |>
    httr2::req_body_json(
      list(
        displayName = name,
        definition = list(
          format = "ipynb",
          parts = list(list(
            path = "notebook-content.ipynb",
            payloadType = "InlineBase64",
            payload = jsonlite::base64_enc(charToRaw(content))
          ))
        )
      ),
      auto_unbox = TRUE
    )
  operation <- .fabric_operation_submit(
    request,
    credential,
    result_expected = TRUE
  )
  fixture <- fabric_operation_result(operation, timeout = 600)$value
  on.exit(
    .httr2_perform(
      httr2::request(paste0(base, "/items/", fixture$id)) |>
        httr2::req_method("DELETE"),
      credential = credential,
      audience = .fabric_audience$fabric
    ),
    add = TRUE
  )
  marker <- paste0("jupyter_", .fabric_lakehouse_staging_id())
  job <- fabric_job_run(
    fixture$id,
    workspace = manifest$workspace_id,
    item_type = "Notebook",
    parameters = list(marker = marker),
    execution_data = list(
      compute = "Jupyter",
      computeConfiguration = list(numCores = 2L)
    ),
    token = token
  )
  on.exit(try(fabric_job_cancel(job), silent = TRUE), add = TRUE)
  completed <- fabric_job_wait(
    job,
    timeout = 900,
    cancel_on_timeout = TRUE,
    notebook_details = TRUE
  )
  expect_identical(completed$status, "Completed")
  expect_identical(
    completed$exit_value,
    paste0("fabricqueryr-job-success:", marker)
  )
})
