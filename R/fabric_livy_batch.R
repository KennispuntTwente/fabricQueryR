# Livy batch R6 object ------------------------------------------------------

#' A Microsoft Fabric Livy batch job
#'
#' Instances are returned by [fabric_livy_batch_submit()]. Use `$status()` to
#' refresh metadata, `$wait()` to block until completion, `$logs()`/`$result()`
#' to inspect the outcome, and `$cancel()` to request cancellation.
#'
#' @field id Fabric batch ID.
#' @field url Batch lifecycle URL.
#' @field state Latest batch state.
#' @field response Latest raw service response.
#' @field cancel_requested Whether `$cancel()` was called successfully.
#' @field submitted_local Local submission timestamp.
#' @field completed_local Local completion timestamp.
#' @field verbose Whether lifecycle messages are enabled.
#' @format An [R6::R6Class] generator.
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
    #' [fabric_livy_batch_submit()].
    #' @param response Initial batch response.
    #' @param url Batch collection URL.
    #' @param credential Internal authentication credential.
    #' @param verbose Whether to emit lifecycle messages.
    #' @returns A new batch object.
    initialize = function(response, url, credential, verbose = TRUE) {
      self$id <- as.character(response$id %||% "")
      fabric_livy_check_string(self$id, "Livy batch response id")
      self$url <- paste0(url, "/", self$id)
      self$state <- response$state
      self$response <- response
      self$submitted_local <- Sys.time()
      self$verbose <- verbose
      private$credential <- credential
      invisible(self)
    },

    #' @description Print a concise batch summary.
    #' @param ... Unused.
    #' @returns `self`, invisibly.
    print = function(...) {
      cat("<Fabric Livy batch>\n")
      cat("  id: ", self$id, "\n", sep = "")
      cat("  state: ", self$state %||% "<unknown>", "\n", sep = "")
      cat("  cancel requested: ", self$cancel_requested, "\n", sep = "")
      invisible(self)
    },

    #' @description Retrieve current batch metadata.
    #' @param refresh Whether to retrieve current state from Fabric.
    #' @returns The raw batch response list.
    status = function(refresh = TRUE) {
      fabric_livy_check_flag(refresh, "refresh")
      if (isTRUE(refresh)) {
        self$response <- fabric_livy_json(
          "GET",
          self$url,
          private$credential
        )
        self$state <- self$response$state %||% self$state
      }
      self$response
    },

    #' @description Wait for the batch to reach a terminal state.
    #' @param timeout Maximum wait in seconds.
    #' @param poll_interval Polling interval in seconds.
    #' @param error_on_failure Raise a structured error for a failed batch.
    #' @param cancel_on_timeout Request cancellation before raising a timeout.
    #' @returns `self`, invisibly.
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
      repeat {
        response <- self$status()
        state <- fabric_livy_state(response)
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
          self$completed_local <- Sys.time()
          if (failed && isTRUE(error_on_failure)) {
            fabric_livy_abort_batch(response)
          }
          return(invisible(self))
        }
        if (Sys.time() >= deadline) {
          if (isTRUE(cancel_on_timeout)) {
            try(self$cancel(), silent = TRUE)
          }
          rlang::abort("Timed out waiting for the Livy batch")
        }
        Sys.sleep(poll_interval)
      }
    },

    #' @description Return available Spark driver log lines.
    #' @param refresh Whether to retrieve current state from Fabric.
    #' @returns A character vector.
    logs = function(refresh = TRUE) {
      response <- self$status(refresh = refresh)
      as.character(response$log %||% character())
    },

    #' @description Return structured batch metadata and logs.
    #' @param refresh Whether to retrieve current state from Fabric.
    #' @param error_on_failure Raise a structured error for a failed batch.
    #' @returns A `fabric_livy_batch_result` list.
    result = function(refresh = TRUE, error_on_failure = TRUE) {
      response <- self$status(refresh = refresh)
      state <- fabric_livy_state(response)
      result <- tolower(response$result %||% "")
      fabric_state <- tolower(
        response$fabricBatchStateInfo$state %||% ""
      )
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

    #' @description Request batch cancellation.
    #' @returns `TRUE`, invisibly, after Fabric accepts the request.
    cancel = function() {
      fabric_livy_ok(
        "DELETE",
        self$url,
        private$credential,
        idempotent = TRUE
      )
      self$cancel_requested <- TRUE
      invisible(TRUE)
    }
  ),
  private = list(
    credential = NULL
  ),
  cloneable = FALSE
)

#' Submit a Microsoft Fabric Livy batch job
#'
#' Starts a complete Spark application file stored in OneLake or ADLS. Unlike an
#' interactive Livy session, a batch has its own application lifecycle and is a
#' good fit for repeatable scripts and unattended processing.
#'
#' @param livy_url A copied Livy connection URL, Livy API base URL, or enriched
#'   Lakehouse record. Copy the batch-job URL from **Lakehouse settings > Livy
#'   endpoint**, or use a row from [fabric_lakehouses()].
#' @param file ABFS/ABFSS URI of the main Python, R, or Java/Scala application
#'   file. After uploading a script under a Lakehouse's `Files/` area, its
#'   **Properties** dialog can copy this path.
#' @param name Optional readable job name shown in Fabric monitoring.
#' @param class_name Main class for a Java/Scala application; leave `NULL` for
#'   Python or R scripts.
#' @param args Optional character vector of command-line arguments passed to the
#'   application.
#' @param jars Optional JAR dependency URIs.
#' @param files Optional supporting-file URIs copied to the job.
#' @param py_files Optional Python dependency URIs, such as `.py` or `.zip`
#'   files.
#' @param archives Optional archive URIs that Spark should unpack.
#' @param conf Optional named list of Spark settings or application-specific
#'   values.
#' @param environment_id Optional GUID of a published Fabric Environment whose
#'   libraries and Spark settings should be used.
#' @param target_lakehouse_id Optional Lakehouse GUID made available as
#'   `spark.targetLakehouse`. Use this when the application needs an explicit
#'   default Lakehouse context.
#' @param tags Optional named list of string labels for monitoring.
#' @param driver_memory,executor_memory Optional Spark memory values such as
#'   `"4g"`. Leave `NULL` to use Fabric defaults.
#' @param driver_cores,executor_cores,num_executors Optional Spark resource
#'   counts. Larger values consume more capacity; leave `NULL` unless the
#'   workload has been sized deliberately.
#' @param tenant_id Microsoft Entra tenant ID. Defaults to
#'   `FABRICQUERYR_TENANT_ID`.
#' @param client_id Microsoft Entra application/client ID. Defaults to
#'   `FABRICQUERYR_CLIENT_ID`, then the Azure CLI application ID.
#' @param token Optional `AzureAuth::AzureToken`, bearer-token string, or
#'   token-provider function. With `NULL`, `AzureAuth` reuses a matching cached
#'   token or starts its normal interactive login flow.
#' @param auth_args Named list of additional arguments passed to
#'   [AzureAuth::get_azure_token()].
#' @param verbose Logical. Show submission and lifecycle messages.
#' @param wait Logical. `FALSE` returns immediately so other R work can
#'   continue; `TRUE` waits for a terminal state before returning the same
#'   object.
#' @param timeout Maximum seconds to wait when `wait = TRUE`.
#' @param poll_interval Seconds between status checks when waiting.
#'
#' @return A [FabricLivyBatch] R6 object. Inspect its `$state`, call
#'   `$result()` for structured metadata and logs, and call `$wait()` later when
#'   submitting with `wait = FALSE`.
#' @details
#' Fabric needs a workspace on supported Fabric capacity and a Lakehouse. The
#' application file must already be accessible through an ABFS/ABFSS URI; this
#' function does not upload a local script. Use [fabric_onelake_upload()] first
#' when needed.
#'
#' Requests use the
#'   `https://api.fabric.microsoft.com/.default` audience. Delegated
#'   authentication requires the Livy Lakehouse execution/read and required
#'   `Code.Access*` scopes documented by Microsoft.
#'
#' @seealso
#' [Microsoft Fabric batch jobs](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-batch)
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
  verbose = TRUE,
  wait = FALSE,
  timeout = 1200,
  poll_interval = 5
) {
  fabric_livy_check_string(file, "file")
  fabric_livy_check_flag(wait, "wait")
  tags <- fabric_livy_normalize_named_list(tags, "tags")
  conf <- fabric_livy_conf(conf, environment_id)
  if (!is.null(target_lakehouse_id)) {
    fabric_livy_check_string(target_lakehouse_id, "target_lakehouse_id")
    conf <- conf %||% list()
    conf[["spark.targetLakehouse"]] <- target_lakehouse_id
  }
  payload <- fabric_livy_payload(
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
  credential <- fabric_livy_credential(
    tenant_id,
    client_id,
    token,
    auth_args
  )
  collection <- fabric_livy_endpoint(
    fabric_livy_resolve_url(livy_url),
    "batches"
  )
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
  if (isTRUE(wait)) {
    batch$wait(timeout = timeout, poll_interval = poll_interval)
  }
  batch
}
