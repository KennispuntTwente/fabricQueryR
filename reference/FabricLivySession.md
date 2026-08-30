# A Microsoft Fabric Livy session

A Livy session keeps Spark running while you submit several pieces of
code Create one with
[`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md),
call `$wait()` once it starts, use `$run()` to execute code, and call
`$close()` when finished. Most users do not need to call this 'R6' class
directly. These lifecycle methods do not have separate free-function
wrappers

## Format

An [R6::R6Class](https://r6.r-lib.org/reference/R6Class.html) generator

## Value

The `FabricLivySession` 'R6' generator.

## Public fields

- `id`:

  Fabric session or high-concurrency acquisition ID

- `url`:

  Session lifecycle URL

- `state`:

  Latest service state

- `response`:

  Latest raw service response

- `closed`:

  Whether `$close()` completed

- `high_concurrency`:

  Whether this is a high-concurrency session

- `session_id`:

  Underlying Livy session ID for HC sessions

- `repl_id`:

  Isolated REPL ID for HC sessions

- `verbose`:

  Whether lifecycle messages are enabled

## Methods

### Public methods

- [`FabricLivySession$new()`](#method-FabricLivySession-initialize)

- [`FabricLivySession$print()`](#method-FabricLivySession-print)

- [`FabricLivySession$status()`](#method-FabricLivySession-status)

- [`FabricLivySession$wait()`](#method-FabricLivySession-wait)

- [`FabricLivySession$submit()`](#method-FabricLivySession-submit)

- [`FabricLivySession$run()`](#method-FabricLivySession-run)

- [`FabricLivySession$statements()`](#method-FabricLivySession-statements)

- [`FabricLivySession$reset_timeout()`](#method-FabricLivySession-reset_timeout)

- [`FabricLivySession$close()`](#method-FabricLivySession-close)

------------------------------------------------------------------------

### `FabricLivySession$new()`

Internal constructor used by
[`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md)

#### Usage

    FabricLivySession$new(
      livy_url,
      credential,
      payload,
      response = NULL,
      high_concurrency = FALSE,
      verbose = TRUE
    )

#### Arguments

- `livy_url`:

  Livy API base or collection URL

- `credential`:

  Internal authentication credential

- `payload`:

  Session creation request body

- `response`:

  Existing session response when attaching

- `high_concurrency`:

  Whether to acquire an HC session

- `verbose`:

  Whether to emit lifecycle messages

#### Returns

A new session object

------------------------------------------------------------------------

### `FabricLivySession$print()`

Print a concise session summary

#### Usage

    FabricLivySession$print(...)

#### Arguments

- `...`:

  Unused

#### Returns

`self`, invisibly

------------------------------------------------------------------------

### `FabricLivySession$status()`

Return the latest session response

#### Usage

    FabricLivySession$status(refresh = TRUE, deadline = NULL)

#### Arguments

- `refresh`:

  Whether to retrieve current state from Fabric

- `deadline`:

  Internal wall-clock deadline for the status request

#### Returns

The raw session response list

------------------------------------------------------------------------

### `FabricLivySession$wait()`

Wait until the session can accept statements

#### Usage

    FabricLivySession$wait(timeout = 600, poll_interval = 3)

#### Arguments

- `timeout`:

  Maximum wait in seconds

- `poll_interval`:

  Polling interval in seconds

#### Returns

`self`, invisibly

------------------------------------------------------------------------

### `FabricLivySession$submit()`

Submit code without waiting for completion

#### Usage

    FabricLivySession$submit(
      code,
      kind = c("spark", "pyspark", "sparkr", "sql"),
      source_id = NULL
    )

#### Arguments

- `code`:

  One string of Spark code

- `kind`:

  Statement language

- `source_id`:

  Optional caller-defined source identifier

#### Returns

A
[FabricLivyStatement](https://kennispunttwente.github.io/fabricQueryR/reference/FabricLivyStatement.md)

------------------------------------------------------------------------

### `FabricLivySession$run()`

Submit code, wait, and return its parsed result

#### Usage

    FabricLivySession$run(
      code,
      kind = c("spark", "pyspark", "sparkr", "sql"),
      source_id = NULL,
      timeout = 600,
      poll_interval = 2
    )

#### Arguments

- `code`:

  One string of Spark code

- `kind`:

  Statement language

- `source_id`:

  Optional caller-defined source identifier

- `timeout`:

  Maximum wait in seconds

- `poll_interval`:

  Polling interval in seconds

#### Returns

A `fabric_livy_statement_result` list

------------------------------------------------------------------------

### `FabricLivySession$statements()`

List every statement in this execution context

#### Usage

    FabricLivySession$statements(page_size = 100L)

#### Arguments

- `page_size`:

  Maximum statements requested per Livy page

#### Returns

The raw Livy statements response

------------------------------------------------------------------------

### `FabricLivySession$reset_timeout()`

Reset a regular session's inactivity timeout

#### Usage

    FabricLivySession$reset_timeout()

#### Returns

`self`, invisibly

------------------------------------------------------------------------

### `FabricLivySession$close()`

Release this session or high-concurrency context

#### Usage

    FabricLivySession$close(deadline = NULL)

#### Arguments

- `deadline`:

  Internal wall-clock deadline for the cleanup request

#### Returns

`TRUE` when closed or `FALSE` when already closed, invisibly

## Examples

``` r
if (FALSE) { # \dontrun{
# fabric_livy_session() creates this class for a discovered Lakehouse
workspace <- fabric_workspaces()[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
session <- fabric_livy_session(lakehouse)
inherits(session, "FabricLivySession")

# Wait before running code, and close the Spark session when finished
session$wait()
session$run("print(1 + 1)", kind = "pyspark")
session$close()
} # }
```
