# Livy batch R6 object ------------------------------------------------------

#' A Microsoft Fabric Livy batch job
#'
#' Instances are returned by [fabric_livy_batch_submit()].
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
#' Submits a Spark application stored in OneLake/ADLS and returns an R6 object
#' for status, logs, result inspection, timeout handling, and cancellation.
#'
#' @param livy_url A copied Livy connection URL, Livy API base URL, or enriched
#'   Lakehouse record.
#' @param file ABFS URI of the application file to execute.
#' @param name Optional job name.
#' @param class_name Optional main class for Java/Scala applications.
#' @param args,jars,files,py_files,archives Optional character vectors passed
#'   to Livy.
#' @param conf Optional named list of Spark settings.
#' @param environment_id Optional Fabric Environment ID.
#' @param target_lakehouse_id Optional Lakehouse ID set as
#'   `spark.targetLakehouse`.
#' @param tags Optional named list of string tags.
#' @param driver_memory,executor_memory Optional Spark memory strings.
#' @param driver_cores,executor_cores,num_executors Optional Spark resource
#'   counts.
#' @param tenant_id Microsoft Entra tenant ID.
#' @param client_id Microsoft Entra application ID.
#' @param access_token Optional Fabric bearer token.
#' @param token_provider Optional callback returning a Fabric bearer token.
#' @param verbose Logical. Emit lifecycle messages.
#' @param wait Logical. Wait for the job to finish before returning.
#' @param timeout,poll_interval Wait controls in seconds.
#'
#' @return A [FabricLivyBatch].
#' @details Requests use the
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
  access_token = NULL,
  token_provider = NULL,
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
    access_token,
    token_provider
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
