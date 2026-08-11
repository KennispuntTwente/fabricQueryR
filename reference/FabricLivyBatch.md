# A Microsoft Fabric Livy batch job

Instances are returned by
[`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md).
Use `$status()` to refresh metadata, `$wait()` to block until
completion, `$logs()`/`$result()` to inspect the outcome, and
`$cancel()` to request cancellation.

## Format

An [R6::R6Class](https://r6.r-lib.org/reference/R6Class.html) generator.

## Public fields

- `id`:

  Fabric batch ID.

- `url`:

  Batch lifecycle URL.

- `state`:

  Latest batch state.

- `response`:

  Latest raw service response.

- `cancel_requested`:

  Whether `$cancel()` was called successfully.

- `submitted_local`:

  Local submission timestamp.

- `completed_local`:

  Local completion timestamp.

- `verbose`:

  Whether lifecycle messages are enabled.

## Methods

### Public methods

- [`FabricLivyBatch$new()`](#method-FabricLivyBatch-initialize)

- [`FabricLivyBatch$print()`](#method-FabricLivyBatch-print)

- [`FabricLivyBatch$status()`](#method-FabricLivyBatch-status)

- [`FabricLivyBatch$wait()`](#method-FabricLivyBatch-wait)

- [`FabricLivyBatch$logs()`](#method-FabricLivyBatch-logs)

- [`FabricLivyBatch$result()`](#method-FabricLivyBatch-result)

- [`FabricLivyBatch$cancel()`](#method-FabricLivyBatch-cancel)

------------------------------------------------------------------------

### `FabricLivyBatch$new()`

Internal constructor used by
[`fabric_livy_batch_submit()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_batch_submit.md).

#### Usage

    FabricLivyBatch$new(response, url, credential, verbose = TRUE)

#### Arguments

- `response`:

  Initial batch response.

- `url`:

  Batch collection URL.

- `credential`:

  Internal authentication credential.

- `verbose`:

  Whether to emit lifecycle messages.

#### Returns

A new batch object.

------------------------------------------------------------------------

### `FabricLivyBatch$print()`

Print a concise batch summary.

#### Usage

    FabricLivyBatch$print(...)

#### Arguments

- `...`:

  Unused.

#### Returns

`self`, invisibly.

------------------------------------------------------------------------

### `FabricLivyBatch$status()`

Retrieve current batch metadata.

#### Usage

    FabricLivyBatch$status(refresh = TRUE, deadline = NULL)

#### Arguments

- `refresh`:

  Whether to retrieve current state from Fabric.

- `deadline`:

  Internal wall-clock deadline for the status request.

#### Returns

The raw batch response list.

------------------------------------------------------------------------

### `FabricLivyBatch$wait()`

Wait for the batch to reach a terminal state.

#### Usage

    FabricLivyBatch$wait(
      timeout = 1200,
      poll_interval = 5,
      error_on_failure = TRUE,
      cancel_on_timeout = FALSE
    )

#### Arguments

- `timeout`:

  Maximum wait in seconds.

- `poll_interval`:

  Polling interval in seconds.

- `error_on_failure`:

  Raise a structured error for a failed batch.

- `cancel_on_timeout`:

  Request cancellation before raising a timeout.

#### Returns

`self`, invisibly.

------------------------------------------------------------------------

### `FabricLivyBatch$logs()`

Return available Spark driver log lines.

#### Usage

    FabricLivyBatch$logs(refresh = TRUE)

#### Arguments

- `refresh`:

  Whether to retrieve current state from Fabric.

#### Returns

A character vector.

------------------------------------------------------------------------

### `FabricLivyBatch$result()`

Return structured batch metadata and logs.

#### Usage

    FabricLivyBatch$result(refresh = TRUE, error_on_failure = TRUE)

#### Arguments

- `refresh`:

  Whether to retrieve current state from Fabric.

- `error_on_failure`:

  Raise a structured error for a failed batch.

#### Returns

A `fabric_livy_batch_result` list.

------------------------------------------------------------------------

### `FabricLivyBatch$cancel()`

Request batch cancellation.

#### Usage

    FabricLivyBatch$cancel(deadline = NULL)

#### Arguments

- `deadline`:

  Internal wall-clock deadline for the cancellation request.

#### Returns

`TRUE`, invisibly, after Fabric accepts the request.
