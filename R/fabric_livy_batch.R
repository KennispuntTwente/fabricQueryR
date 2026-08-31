#' Submit a Microsoft Fabric Livy batch job
#'
#' Runs a complete Python, R, or Java/Scala Spark application stored in OneLake
#' or ADLS. Use this for repeatable scripts and unattended processing; use
#' [fabric_livy_session()] when several interactive statements should share
#' variables and Spark state
#'
#' @param livy_url A copied Livy connection URL, Livy API base URL, or enriched
#'   Lakehouse object. Copy the batch-job URL from **Lakehouse settings > Livy
#'   endpoint**, or use an item from [fabric_lakehouses()]
#' @param file Absolute ABFS/ABFSS URI of the main Python, R, or Java/Scala
#'   application file. It must contain a filesystem/container, host, and
#'   non-root path, without a password, port, query, fragment, backslash, or dot
#'   path segment. After uploading a script under a Lakehouse's `Files/` area,
#'   its **Properties** dialog can copy this path. Spaces in path segments must
#'   be percent-encoded as `%20`; raw spaces and authority whitespace are invalid
#' @param name Optional readable job name shown in Fabric monitoring
#' @param class_name Main class for a Java/Scala application; leave `NULL` for
#'   Python or R scripts
#' @param args Optional character vector of command-line arguments passed to the
#'   application
#' @param jars Optional JAR dependency URIs
#' @param files Optional supporting-file URIs copied to the job
#' @param py_files Optional Python dependency URIs, such as `.py` or `.zip`
#'   files
#' @param archives Optional archive URIs that Spark should unpack
#' @param conf Optional named list of Spark settings or application-specific
#'   values
#' @param environment_id Optional GUID of a published Fabric Environment whose
#'   libraries and Spark settings should be used
#' @param target_lakehouse_id Optional Lakehouse GUID made available as
#'   `spark.targetLakehouse`. Use this when the application needs an explicit
#'   default Lakehouse context
#' @param tags Optional named list of string labels for monitoring
#' @param driver_memory,executor_memory Optional Spark memory values such as
#'   `"4g"`. Leave `NULL` to use Fabric defaults
#' @param driver_cores,executor_cores,num_executors Optional Spark resource
#'   counts. Larger values consume more capacity; leave `NULL` unless the
#'   workload has been sized deliberately
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID
#' @param token Optional access token or token-provider function. Leave `NULL`
#'   to let 'fabricQueryR' use its normal sign-in flow for a Microsoft Fabric
#'   host. A custom `livy_url` requires an explicitly supplied token or provider.
#'   HTTPS validation does not prove ownership or token audience; use a custom
#'   host only when your organization controls it, with a credential issued for
#'   its intended audience
#' @param auth_args Additional sign-in options passed to
#'   [AzureAuth::get_azure_token()]
#' @param audience Optional sign-in scopes. For delegated sign-in, `NULL`
#'   requests the four required Livy scopes listed below. An explicit vector
#'   replaces those defaults, so include every required scope plus any optional
#'   `Code.Access*` scope the Spark code needs. Client credentials require one
#'   `.default` audience
#' @param verbose Logical. Show submission and lifecycle messages
#' @param wait Logical. `FALSE` returns immediately so other R work can
#'   continue; `TRUE` waits for a terminal state before returning the same
#'   object
#' @param timeout Maximum seconds to wait when `wait = TRUE`
#' @param poll_interval Seconds between status checks when waiting
#' @param cancel_on_timeout Logical. When waiting at submission time, request
#'   cancellation if the local timeout expires. Defaults to `TRUE`, so a timed
#'   out call does not normally leave Spark compute running unattended. The
#'   structured timeout condition contains the live [FabricLivyBatch] object in
#'   `handle`, for status checks or cancellation in the current R process, and
#'   stable public metadata in `batch`. A serialized handle intentionally loses
#'   its in-process credential
#'
#' @return A [FabricLivyBatch] 'R6' object. Inspect its `$state`, call
#'   `$result()` for structured metadata and logs, and call `$wait()` later when
#'   submitting with `wait = FALSE`
#' @section Before you submit:
#' Fabric needs a workspace on supported capacity and a Lakehouse. The
#' application file must already be accessible through an ABFS/ABFSS URI; this
#' function does not upload a local script. Use [fabric_onelake_upload()] first
#' when needed
#'
#' Delegated sign-in requires `Lakehouse.Execute.All`, `Lakehouse.Read.All`,
#' `Code.AccessFabric.All`, and `Code.AccessStorage.All`. Add
#' `Code.AccessAzureKeyvault.All`, `Code.AccessAzureDataLake.All`,
#' `Code.AccessAzureDataExplorer.All`, or `Code.AccessSQL.All` only when Spark
#' accesses that Azure service at runtime. The signed-in identity also needs an
#' appropriate workspace role
#'
#' @seealso
#' [Microsoft Fabric batch jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-batch)
#'
#' @examples
#' \dontrun{
#' # Discover the Lakehouse and Python file used by this batch
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#' scripts <- fabric_onelake_list(
#'   workspace,
#'   lakehouse,
#'   path = "Files/jobs"
#' )
#' script <- scripts[grepl("[.]py$", scripts$path), ][1L, ]
#' script_uri <- paste0(
#'   "abfss://", workspace$id, "@onelake.dfs.fabric.microsoft.com/",
#'   lakehouse$id, "/", script$path[[1L]]
#' )
#'
#' # Submit the discovered script and wait for its Spark application to finish
#' batch <- fabric_livy_batch_submit(
#'   lakehouse,
#'   file = script_uri,
#'   wait = TRUE,
#'   cancel_on_timeout = TRUE
#' )
#' batch$result()
#' }
#'
#' @export
fabric_livy_batch_submit <- function(
  livy_url,
  file,
  name = NULL,
  class_name = NULL,
  args = NULL,
  jars = NULL,
  files = NULL,
  py_files = NULL,
  archives = NULL,
  conf = NULL,
  environment_id = NULL,
  target_lakehouse_id = NULL,
  tags = NULL,
  driver_memory = NULL,
  driver_cores = NULL,
  executor_memory = NULL,
  executor_cores = NULL,
  num_executors = NULL,
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv(
    "FABRICQUERYR_CLIENT_ID",
    unset = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"
  ),
  token = NULL,
  auth_args = list(),
  audience = NULL,
  verbose = TRUE,
  wait = FALSE,
  timeout = 1200,
  poll_interval = 5,
  cancel_on_timeout = TRUE
) {
  # 1 Validate batch options -----------------------------------------------------------------------

  # Validate all local settings before submitting an application that cannot be
  # changed after Fabric accepts it

  fabric_livy_validate_abfs_uri(file, "file")
  fabric_livy_check_flag(wait, "wait")
  fabric_livy_check_flag(cancel_on_timeout, "cancel_on_timeout")
  fabric_livy_check_flag(verbose, "verbose")
  fabric_livy_check_number(timeout, "timeout")
  fabric_livy_check_number(poll_interval, "poll_interval")
  fabric_livy_validate_session_fields(
    name = name,
    archives = archives,
    driver_memory = driver_memory,
    driver_cores = driver_cores,
    executor_memory = executor_memory,
    executor_cores = executor_cores,
    num_executors = num_executors
  )
  fabric_livy_check_optional_string(class_name, "class_name")
  fabric_livy_check_string_vector(args, "args", allow_empty_strings = TRUE)
  fabric_livy_check_string_vector(jars, "jars")
  fabric_livy_check_string_vector(files, "files")
  fabric_livy_check_string_vector(py_files, "py_files")
  tags <- fabric_livy_normalize_named_list(tags, "tags")
  conf <- fabric_livy_conf(conf, environment_id)
  if (!is.null(target_lakehouse_id)) {
    fabric_livy_check_guid(target_lakehouse_id, "target_lakehouse_id")
    conf <- conf %||% list()
    conf[["spark.targetLakehouse"]] <- target_lakehouse_id
  }

  # 2 Build the batch request ----------------------------------------------------------------------

  # Omit unset settings so the Fabric environment can provide its defaults

  payload <- Filter(
    Negate(is.null),
    list(
      file = file,
      name = name,
      className = class_name,
      args = args,
      jars = jars,
      files = files,
      pyFiles = py_files,
      archives = archives,
      conf = conf,
      tags = tags,
      driverMemory = driver_memory,
      driverCores = driver_cores,
      executorMemory = executor_memory,
      executorCores = executor_cores,
      numExecutors = num_executors
    )
  )

  # Resolve authentication separately from the service route
  endpoint <- fabric_livy_resolve_url(livy_url)
  fabric_require_explicit_custom_token(endpoint, token, "livy_url")
  credential <- fabric_livy_credential(
    tenant_id,
    client_id,
    token,
    auth_args,
    audience
  )

  collection <- fabric_livy_endpoint(
    endpoint,
    "batches"
  )

  # 3 Submit the application -----------------------------------------------------------------------

  # Submit the application only after authentication and payload fields are ready

  inform(verbose, "Submitting Fabric Livy batch")
  response <- fabric_livy_json(
    "POST",
    collection,
    credential,
    payload = payload,
    idempotent = FALSE
  )
  batch <- FabricLivyBatch$new(
    response = response,
    url = collection,
    credential = credential,
    verbose = verbose
  )

  # 4 Optionally wait for completion ---------------------------------------------------------------

  # Wait here only when the caller requested a completed batch

  if (isTRUE(wait)) {
    batch$wait(
      timeout = timeout,
      poll_interval = poll_interval,
      cancel_on_timeout = cancel_on_timeout
    )
  }
  batch
}

# Livy batch R6 object -----------------------------------------------------------------------------

#' A Microsoft Fabric Livy batch job
#'
#' Represents a Spark application submitted with [fabric_livy_batch_submit()]
#' Use `$wait()` to wait for completion, `$result()` or `$logs()` to inspect the
#' outcome, and `$cancel()` to request cancellation. Most users do not need to
#' call this 'R6' class directly. These lifecycle methods do not have separate
#' free-function wrappers
#'
#' @field id Fabric batch ID
#' @field url Batch lifecycle URL
#' @field state Latest batch state
#' @field response Latest raw service response
#' @field cancel_requested Whether `$cancel()` was called successfully
#' @field submitted_local Local submission timestamp
#' @field completed_local Local completion timestamp
#' @field verbose Whether lifecycle messages are enabled
#' @format An [R6::R6Class] generator
#' @return The `FabricLivyBatch` 'R6' generator.
#' @examples
#' \dontrun{
#' # fabric_livy_batch_submit() returns this class for a submitted Spark job
#' workspace <- fabric_workspaces()[[1L]]
#' lakehouse <- fabric_lakehouses(workspace)[[1L]]
#' scripts <- fabric_onelake_list(workspace, lakehouse, "Files/jobs")
#' script <- scripts[grepl("[.]py$", scripts$path), ][1L, ]
#' script_uri <- paste0(
#'   "abfss://", workspace$id, "@onelake.dfs.fabric.microsoft.com/",
#'   lakehouse$id, "/", script$path[[1L]]
#' )
#' batch <- fabric_livy_batch_submit(
#'   lakehouse,
#'   file = script_uri
#' )
#' inherits(batch, "FabricLivyBatch")
#'
#' # Wait for completion, then inspect the application result and logs
#' batch$wait()
#' batch$result()
#' batch$logs()
#' }
#' @export
FabricLivyBatch <- R6::R6Class(
  classname = "FabricLivyBatch",
  public = list(
    id = NULL,
    url = NULL,
    state = NULL,
    response = NULL,
    cancel_requested = FALSE,
    submitted_local = NULL,
    completed_local = NULL,
    verbose = TRUE,

    #' @description Internal constructor used by
    #' [fabric_livy_batch_submit()]
    #' @param response Initial batch response
    #' @param url Batch collection URL
    #' @param credential Internal authentication credential
    #' @param verbose Whether to emit lifecycle messages
    #' @returns A new batch object
    initialize = function(response, url, credential, verbose = TRUE) {
      self$id <- as.character(response$id %||% "")
      fabric_livy_check_string(self$id, "Livy batch response id")
      self$url <- paste0(url, "/", self$id)
      self$state <- response$state
      self$response <- response
      self$submitted_local <- Sys.time()
      self$verbose <- verbose
      credential_reference <- fabric_livy_credential_reference(credential)
      private$credential_ref <- credential_reference$reference
      private$credential_key <- credential_reference$key
      invisible(self)
    },

    #' @description Print a concise batch summary
    #' @param ... Unused
    #' @returns `self`, invisibly
    print = function(...) {
      .fabric_print(
        "Fabric Livy batch",
        list(
          id = self$id,
          state = self$state %||% "<unknown>",
          `cancel requested` = self$cancel_requested
        )
      )
      invisible(self)
    },

    #' @description Retrieve current batch metadata
    #' @param refresh Whether to retrieve current state from Fabric
    #' @param deadline Internal wall-clock deadline for the status request
    #' @returns The raw batch response list
    status = function(refresh = TRUE, deadline = NULL) {
      fabric_livy_check_flag(refresh, "refresh")
      if (isTRUE(refresh)) {
        self$response <- fabric_livy_json(
          "GET",
          self$url,
          fabric_livy_handle_credential(private$credential_ref),
          deadline = deadline
        )
        self$state <- self$response$state %||% self$state
      }
      self$response
    },

    #' @description Wait for the batch to reach a terminal state
    #' @param timeout Maximum wait in seconds
    #' @param poll_interval Polling interval in seconds
    #' @param error_on_failure Raise a structured error for a failed batch
    #' @param cancel_on_timeout Request cancellation before raising a timeout
    #' @returns `self`, invisibly
    wait = function(
      timeout = 1200,
      poll_interval = 5,
      error_on_failure = TRUE,
      cancel_on_timeout = FALSE
    ) {
      fabric_livy_check_number(timeout, "timeout")
      fabric_livy_check_number(poll_interval, "poll_interval")
      fabric_livy_check_flag(error_on_failure, "error_on_failure")
      fabric_livy_check_flag(cancel_on_timeout, "cancel_on_timeout")
      deadline <- Sys.time() + timeout
      progress <- .fabric_poll_progress(
        "Fabric Livy batch",
        self$id,
        verbose = self$verbose
      )
      repeat {
        if (fabric_livy_remaining(deadline) <= 0) {
          private$abort_timeout(deadline, cancel_on_timeout)
        }
        response <- tryCatch(
          self$status(deadline = deadline),
          fabric_http_deadline_error = function(error) {
            private$abort_timeout(deadline, cancel_on_timeout)
          }
        )
        state <- fabric_livy_state(response)
        .fabric_poll_progress_update(progress, state)
        result <- tolower(response$result %||% "")
        fabric_state <- tolower(
          response$fabricBatchStateInfo$state %||% ""
        )
        succeeded <- state %in%
          .fabric_livy_batch_success_states ||
          identical(result, "succeeded")
        failed <- state %in%
          .fabric_livy_batch_failure_states ||
          result %in% c("failed", "cancelled", "canceled") ||
          fabric_state %in% c("error", "cancelled", "canceled", "expired")
        if (succeeded || failed) {
          self$completed_local <- self$completed_local %||% Sys.time()
          if (failed && isTRUE(error_on_failure)) {
            fabric_livy_abort_batch(response)
          }

          .fabric_poll_progress_done(progress)
          return(invisible(self))
        }
        fabric_livy_poll_sleep(deadline, poll_interval)
      }
    },

    #' @description Return available Spark driver log lines
    #' @param refresh Whether to retrieve current state from Fabric
    #' @returns A character vector
    logs = function(refresh = TRUE) {
      response <- self$status(refresh = refresh)
      as.character(response$log %||% character())
    },

    #' @description Return structured batch metadata and logs
    #' @param refresh Whether to retrieve current state from Fabric
    #' @param error_on_failure Raise a structured error for a failed batch
    #' @returns A `fabric_livy_batch_result` list
    result = function(refresh = TRUE, error_on_failure = TRUE) {
      # 1 Read and classify current status ---------------------------------------------------------

      # Read and classify current status once so later checks use a consistent view

      fabric_livy_check_flag(error_on_failure, "error_on_failure")
      response <- self$status(refresh = refresh)
      state <- fabric_livy_state(response)
      result <- tolower(response$result %||% "")
      fabric_state <- tolower(
        response$fabricBatchStateInfo$state %||% ""
      )
      terminal <- state %in%
        c(
          .fabric_livy_batch_success_states,
          .fabric_livy_batch_failure_states
        ) ||
        result %in% c("succeeded", "failed", "cancelled", "canceled") ||
        fabric_state %in% c("error", "cancelled", "canceled", "expired")
      if (terminal) {
        self$completed_local <- self$completed_local %||% Sys.time()
      }

      # 2 Raise an optional failure error ----------------------------------------------------------

      # Turn the final state into clear output for the caller

      if (
        isTRUE(error_on_failure) &&
          (state %in%
            .fabric_livy_batch_failure_states ||
            result %in% c("failed", "cancelled", "canceled") ||
            fabric_state %in%
              c(
                "error",
                "cancelled",
                "canceled",
                "expired"
              ))
      ) {
        fabric_livy_abort_batch(response)
      }

      # 3 Return structured batch details ----------------------------------------------------------

      # Return structured batch details in the stable form expected by the caller

      structure(
        list(
          id = response$id,
          state = response$state,
          result = response$result %||% NULL,
          app_id = response$appId %||% NULL,
          logs = response$log %||% character(),
          error_info = response$errorInfo %||% list(),
          cancellation_reason = response$cancellationReason %||% NULL,
          submitted_local = self$submitted_local,
          completed_local = self$completed_local,
          url = self$url,
          raw = response
        ),
        class = c("fabric_livy_batch_result", "list")
      )
    },

    #' @description Request batch cancellation
    #' @param deadline Internal wall-clock deadline for the cancellation request
    #' @returns `TRUE`, invisibly, after Fabric accepts the request
    cancel = function(deadline = NULL) {
      fabric_livy_ok(
        "DELETE",
        self$url,
        fabric_livy_handle_credential(private$credential_ref),
        idempotent = TRUE,
        accepted_status = 404L,
        deadline = deadline
      )
      self$cancel_requested <- TRUE
      invisible(TRUE)
    }
  ),
  private = list(
    credential_ref = NULL,
    credential_key = NULL,

    # Handle a wait deadline, optionally asking Fabric to cancel the batch
    # It receives the deadline and cancel flag, then always raises a timeout
    abort_timeout = function(deadline, cancel_on_timeout) {
      cancellation <- if (isTRUE(cancel_on_timeout)) {
        # The wait deadline is necessarily exhausted here. Give the best-effort
        # cleanup request its own short budget so the DELETE can actually leave
        # the process without allowing cleanup to block indefinitely
        cleanup_timeout <- getOption("fabricqueryr.livy.cleanup_timeout", 30)
        fabric_livy_check_number(cleanup_timeout, "Livy cleanup timeout")
        cleanup_deadline <- Sys.time() + cleanup_timeout
        tryCatch(
          {
            self$cancel(deadline = cleanup_deadline)
            list(accepted = TRUE, error = NULL)
          },
          error = function(error) list(accepted = FALSE, error = error)
        )
      } else {
        list(accepted = NULL, error = NULL)
      }
      fabric_livy_abort_timeout(
        "batch",
        self,
        self$response,
        cancel_accepted = cancellation$accepted,
        cancel_error = cancellation$error
      )
    }
  ),
  cloneable = FALSE
)
