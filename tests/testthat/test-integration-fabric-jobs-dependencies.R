# Fabric integration coverage: jobs dependencies
test_that("Notebook pyFiles jars and archives are available to executed code", {
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  lake <- fabric_test_manifest_item(manifest, "TestLakehouse")
  folder <- paste0(
    "Files/fabricqueryr_dependencies_",
    .fabric_lakehouse_staging_id()
  )
  uri <- paste0(
    "abfss://",
    manifest$workspace_id,
    "@onelake.dfs.fabric.microsoft.com/",
    lake$id,
    "/",
    folder
  )
  withr::defer(try(
    fabric_onelake_delete(
      manifest$workspace_id,
      lake$id,
      folder,
      recursive = TRUE,
      confirm = TRUE,
      token = token
    ),
    silent = TRUE
  ))
  files <- c("job-dependency.py", "job-resource.jar", "job-archive.zip")
  remote <- c("fabricqueryr_dependency.py", "resource.jar", "archive.zip")
  for (i in seq_along(files)) {
    fabric_onelake_upload(
      manifest$workspace_id,
      lake$id,
      paste(folder, remote[[i]], sep = "/"),
      source = testthat::test_path("..", "fixtures", files[[i]]),
      token = token
    )
  }
  code <- paste(
    "import fabricqueryr_dependency as dependency",
    "from pyspark import SparkFiles",
    "from pathlib import Path",
    "import notebookutils",
    "assert dependency.VALUE == 'fabricqueryr-dependency-73'",
    "def read_archive(_):",
    "    from pathlib import Path",
    "    from pyspark import SparkFiles",
    "    return Path(SparkFiles.get('archive.zip'), 'marker.txt').read_text()",
    "assert spark.sparkContext.parallelize([1], 1).map(read_archive).collect() == [dependency.VALUE]",
    "resource = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader().getResourceAsStream('fabricqueryr-resource.txt')",
    "assert resource is not None, 'jars resource missing'",
    "assert spark._jvm.java.util.Scanner(resource).useDelimiter('\\\\A').next() == dependency.VALUE",
    sep = "\n"
  )
  code <- paste(
    "import notebookutils, traceback",
    "result = 'fabricqueryr-dependency-73'",
    "try:",
    paste0("    ", strsplit(code, "\n", fixed = TRUE)[[1L]], collapse = "\n"),
    "except Exception:",
    "    result = traceback.format_exc()",
    "notebookutils.notebook.exit(result)",
    sep = "\n"
  )
  notebook <- fabric_test_probe_notebook(manifest$workspace_id, code, token)
  job <- fabric_job_run(
    notebook,
    workspace = manifest$workspace_id,
    item_type = "Notebook",
    execution_data = list(
      compute = "Spark",
      computeConfiguration = list(
        pyFiles = paste0(uri, "/fabricqueryr_dependency.py"),
        jars = paste0(uri, "/resource.jar"),
        archives = paste0(uri, "/archive.zip")
      )
    ),
    token = token
  )
  withr::defer(try(fabric_job_cancel(job), silent = TRUE))
  completed <- fabric_job_wait(
    job,
    timeout = 1200,
    cancel_on_timeout = TRUE,
    notebook_details = TRUE
  )
  expect_identical(completed$status, "Completed")
  expect_identical(completed$exit_value, "fabricqueryr-dependency-73")
})

test_that("Notebook cross-workspace defaults select the requested Lakehouse", {
  workspace <- fabric_test_feature_environment(
    "job-options",
    "FABRIC_TEST_CROSS_WORKSPACE_ID"
  )
  lake <- fabric_test_feature_environment(
    "job-options",
    "FABRIC_TEST_CROSS_LAKEHOUSE_ID"
  )
  manifest <- fabric_test_manifest()
  expect_false(identical(workspace, manifest$workspace_id))
  token <- fabric_test_token_provider()
  code <- paste(
    "import json, notebookutils",
    "notebookutils.notebook.exit(json.dumps(dict(notebookutils.runtime.context)))",
    sep = "\n"
  )
  notebook <- fabric_test_probe_notebook(manifest$workspace_id, code, token)
  job <- fabric_job_run(
    notebook,
    workspace = manifest$workspace_id,
    item_type = "Notebook",
    default_lakehouse = lake,
    default_lakehouse_workspace = workspace,
    token = token
  )
  withr::defer(try(fabric_job_cancel(job), silent = TRUE))
  completed <- fabric_job_wait(
    job,
    timeout = 1200,
    notebook_details = TRUE,
    cancel_on_timeout = TRUE
  )
  context <- jsonlite::fromJSON(completed$exit_value)
  expect_identical(context$defaultLakehouseId, lake)
  expect_identical(context$defaultLakehouseWorkspaceId, workspace)
})

test_that("Notebook custom pool sizing and mounts affect the running session", {
  configuration <- jsonlite::fromJSON(
    fabric_test_feature_environment(
      "job-options",
      "FABRIC_TEST_NOTEBOOK_COMPUTE_JSON"
    ),
    simplifyVector = FALSE
  )
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  compute <- configuration$computeConfiguration
  expect_true(all(
    c(
      "instancePool",
      "driverCores",
      "executorCores",
      "driverMemory",
      "executorMemory",
      "numExecutors",
      "mountPoints"
    ) %in%
      names(compute)
  ))
  # The probe reads runtime settings and mounted bytes rather than echoing inputs.
  code <- paste(
    "import json, notebookutils",
    "from pathlib import Path",
    "conf = spark.sparkContext.getConf()",
    paste0(
      "path = notebookutils.fs.getMountPath(",
      jsonlite::toJSON(
        compute$mountPoints[[1L]]$mountPointPath,
        auto_unbox = TRUE
      ),
      ")"
    ),
    paste0(
      "text = (Path(path) / ",
      jsonlite::toJSON(configuration$file, auto_unbox = TRUE),
      ").read_text()"
    ),
    "notebookutils.notebook.exit(json.dumps({'settings': dict(conf.getAll()), 'text': text}))",
    sep = "\n"
  )
  notebook <- fabric_test_probe_notebook(manifest$workspace_id, code, token)
  job <- fabric_job_run(
    notebook,
    workspace = manifest$workspace_id,
    item_type = "Notebook",
    execution_data = list(compute = "Spark", computeConfiguration = compute),
    token = token
  )
  withr::defer(try(fabric_job_cancel(job), silent = TRUE))
  completed <- fabric_job_wait(
    job,
    timeout = 1200,
    notebook_details = TRUE,
    cancel_on_timeout = TRUE
  )
  result <- jsonlite::fromJSON(completed$exit_value)
  mapping <- c(
    driverCores = "spark.driver.cores",
    executorCores = "spark.executor.cores",
    driverMemory = "spark.driver.memory",
    executorMemory = "spark.executor.memory",
    numExecutors = "spark.executor.instances"
  )
  for (field in names(mapping)) {
    expect_identical(
      result$settings[[mapping[[field]]]],
      as.character(compute[[field]])
    )
  }
  expect_identical(
    result$settings[[configuration$pool_setting]],
    configuration$pool_expected
  )
  expect_identical(result$text, configuration$expected_text)
})

test_that("DataWarehouse compute executes the configured SQL notebook", {
  notebook <- fabric_test_feature_environment(
    "job-options",
    "FABRIC_TEST_SQL_NOTEBOOK_ID"
  )
  warehouse <- fabric_test_feature_environment(
    "job-options",
    "FABRIC_TEST_SQL_NOTEBOOK_WAREHOUSE_ID"
  )
  manifest <- fabric_test_manifest()
  token <- fabric_test_token_provider()
  marker <- .fabric_lakehouse_staging_id()
  item <- fabric_item(
    manifest$workspace_id,
    warehouse,
    type = "Warehouse",
    token = token
  )
  con <- fabric_sql_connect(
    item,
    token = token,
    read_only = FALSE,
    verbose = FALSE
  )
  withr::defer(DBI::dbDisconnect(con))
  # Fixture SQL notebook inserts its marker parameter into this dedicated table.
  sql <- paste0(
    "SELECT marker FROM dbo.fabricqueryr_sql_notebook_probe WHERE marker = '",
    marker,
    "'"
  )
  withr::defer(try(
    DBI::dbExecute(
      con,
      paste0(
        "DELETE FROM dbo.fabricqueryr_sql_notebook_probe WHERE marker = '",
        marker,
        "'"
      )
    ),
    silent = TRUE
  ))
  expect_equal(
    nrow(DBI::dbGetQuery(con, sql)),
    0L
  )
  job <- fabric_job_run(
    notebook,
    workspace = manifest$workspace_id,
    item_type = "Notebook",
    parameters = list(marker = marker),
    execution_data = list(compute = "DataWarehouse"),
    token = token
  )
  withr::defer(try(fabric_job_cancel(job), silent = TRUE))
  expect_identical(
    fabric_job_wait(job, timeout = 600, cancel_on_timeout = TRUE)$status,
    "Completed"
  )
  expect_identical(
    DBI::dbGetQuery(con, sql)$marker,
    marker
  )
})
