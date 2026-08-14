# A statement submitted to a Fabric Livy session

Represents one piece of code submitted to a
[FabricLivySession](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivySession.md).
Call `$wait()` and then `$result()` to retrieve its output. For the
usual submit-and-wait workflow, use the session's `$run()` method
instead

## Format

An [R6::R6Class](https://r6.r-lib.org/reference/R6Class.html) generator

## Public fields

- `id`:

  Numeric Livy statement ID

- `url`:

  Statement lifecycle URL

- `state`:

  Latest statement state

- `response`:

  Latest raw service response

- `started_local`:

  Local submission timestamp

- `completed_local`:

  Local completion timestamp

- `verbose`:

  Whether lifecycle messages are enabled

## Methods

### Public methods

- [`FabricLivyStatement$new()`](#method-FabricLivyStatement-initialize)

- [`FabricLivyStatement$print()`](#method-FabricLivyStatement-print)

- [`FabricLivyStatement$status()`](#method-FabricLivyStatement-status)

- [`FabricLivyStatement$wait()`](#method-FabricLivyStatement-wait)

- [`FabricLivyStatement$result()`](#method-FabricLivyStatement-result)

- [`FabricLivyStatement$cancel()`](#method-FabricLivyStatement-cancel)

------------------------------------------------------------------------

### `FabricLivyStatement$new()`

Internal constructor used by `FabricLivySession$submit()`

#### Usage

    FabricLivyStatement$new(session, response, url, credential, verbose = TRUE)

#### Arguments

- `session`:

  Parent
  [FabricLivySession](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivySession.md)

- `response`:

  Initial statement response

- `url`:

  Statement lifecycle URL

- `credential`:

  Internal authentication credential

- `verbose`:

  Whether to emit lifecycle messages

#### Returns

A new statement object

------------------------------------------------------------------------

### `FabricLivyStatement$print()`

Print a concise statement summary

#### Usage

    FabricLivyStatement$print(...)

#### Arguments

- `...`:

  Unused

#### Returns

`self`, invisibly

------------------------------------------------------------------------

### `FabricLivyStatement$status()`

Retrieve statement state and available output

#### Usage

    FabricLivyStatement$status(refresh = TRUE, deadline = NULL)

#### Arguments

- `refresh`:

  Whether to retrieve current state from Fabric

- `deadline`:

  Internal wall-clock deadline for the status request

#### Returns

The raw statement response list

------------------------------------------------------------------------

### `FabricLivyStatement$wait()`

Wait for the statement to reach a terminal state

#### Usage

    FabricLivyStatement$wait(
      timeout = 600,
      poll_interval = 2,
      error_on_failure = TRUE
    )

#### Arguments

- `timeout`:

  Maximum wait in seconds

- `poll_interval`:

  Polling interval in seconds

- `error_on_failure`:

  Raise a structured error for failed statements

#### Returns

`self`, invisibly

------------------------------------------------------------------------

### `FabricLivyStatement$result()`

Return parsed output and timing metadata

#### Usage

    FabricLivyStatement$result(refresh = TRUE, error_on_failure = TRUE)

#### Arguments

- `refresh`:

  Whether to retrieve current state from Fabric

- `error_on_failure`:

  Raise a structured error for failed statements

#### Returns

A `fabric_livy_statement_result` list

------------------------------------------------------------------------

### `FabricLivyStatement$cancel()`

Request cancellation of this statement

#### Usage

    FabricLivyStatement$cancel()

#### Returns

The raw cancellation response, invisibly

## Examples

``` r
if (FALSE) { # \dontrun{
# Statements are returned by a session; users do not construct them directly
workspace <- fabric_workspaces()[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
session <- fabric_livy_session(lakehouse)
session$wait()

# Submit code, wait for it, and inspect its result
statement <- session$submit("print(40 + 2)", kind = "pyspark")
inherits(statement, "FabricLivyStatement")
statement$wait()
statement$result()
session$close()
} # }
```
