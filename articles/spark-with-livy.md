# Working with Livy (Spark)

Apache Spark processes data using compute that runs in Fabric. **Livy**
is the service that lets an R program submit Spark code and receive its
status and output. The code runs remotely; it does not run in your local
R process.

Use Spark when a transformation is too large for one computer, needs a
Spark-specific library or format, or already exists as a Spark
application. For a small table read, SQL or a direct Lakehouse reader is
usually simpler and starts faster.

## Before the first call

You need a Fabric workspace on supported capacity, a Lakehouse with a
Livy endpoint, and the **tenant admin setting for the Livy API
enabled**. A delegated caller needs all four of these Microsoft Entra
scopes:

- `Lakehouse.Execute.All`
- `Lakehouse.Read.All`
- `Code.AccessFabric.All`
- `Code.AccessStorage.All`

The signed-in user must be a **Contributor** in the workspace containing
the Livy endpoint and data-source items. For unattended authentication,
add the service principal to the workspace as a Contributor.
`fabricQueryR` requests the four delegated scopes automatically for its
normal interactive sign-in; client-credentials authentication uses the
Fabric `.default` audience.

Discovering the Lakehouse is normally enough; its record carries the
endpoint:

``` r

library(fabricQueryR)

workspaces <- fabric_workspaces()
matches <- Filter(
  \(x) identical(x$displayName, "Analytics workspace"),
  workspaces
)
stopifnot(length(matches) == 1L)
workspace <- matches[[1L]]
lakehouse <- fabric_lakehouses(workspace)[[1L]]
```

If discovery cannot retrieve the endpoint in your environment, copy the
session-job connection string from **Lakehouse settings \> Livy
endpoint** and pass that URL instead.

If Fabric returns HTTP 403, check the tenant setting, the workspace
Contributor role, and—when using delegated authentication—the four
scopes above before changing the endpoint or Spark code. See Microsoft’s
[Livy API setup and
authorization](https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy)
for the current requirements.

## Run one piece of Spark code

[`fabric_livy_query()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_query.md)
is the simplest helper. It starts a temporary session, runs one
statement, waits, and closes the session:

``` r

result <- fabric_livy_query(
  lakehouse,
  kind = "sql",
  code = "SELECT 1 AS id, 'hello from Spark' AS message"
)

result$output$parsed
```

The returned `output$parsed` value is usually a tibble for tabular
results, an R object for JSON, or character output for printed text.

## Choose the language that matches the code

The `kind` argument tells Livy how to interpret `code`:

| `kind`      | Code language     |
|-------------|-------------------|
| `"sql"`     | Spark SQL         |
| `"sparkr"`  | SparkR            |
| `"pyspark"` | Python with Spark |
| `"spark"`   | Scala             |

For example, SparkR code can use the active Spark session and Lakehouse:

``` r

result <- fabric_livy_query(
  lakehouse,
  kind = "sparkr",
  code = paste(
    "df <- sql('SELECT * FROM orders LIMIT 100')",
    "printSchema(df)",
    "showDF(df, numRows = 10)",
    sep = "\n"
  )
)
```

The language selected by `kind` is the language running in Fabric. Local
R variables are not automatically available there; build the submitted
code deliberately and do not insert untrusted text into it.

## Reuse a session for several statements

Starting Spark can take time. Use
[`fabric_livy_session()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_livy_session.md)
when sequential statements need to share variables or cached data:

``` r

session <- fabric_livy_session(lakehouse)
on.exit(session$close(), add = TRUE)

session$wait()
session$run("shared_value = 40", kind = "pyspark")
answer <- session$run("print(shared_value + 2)", kind = "pyspark")
answer$output$parsed
```

Always close a session explicitly. R object cleanup does not make a
network request, so forgetting `$close()` can leave avoidable Spark work
running.

A standard session is right for one R process running a sequence. High
concurrency is an advanced option for several isolated workloads that
may share underlying compute; it is not needed for a few statements in
order.

## Submit a complete application file

Use a Livy batch when the work is a repeatable Python, R, or Java/Scala
script stored in OneLake or ADLS:

``` r

batch <- fabric_livy_batch_submit(
  lakehouse,
  file = paste0(
    "abfss://", workspace$id,
    "@onelake.dfs.fabric.microsoft.com/",
    lakehouse$id,
    ".Lakehouse/Files/jobs/daily_transform.py"
  ),
  name = "daily-transform",
  wait = TRUE,
  timeout = 1800
)

batch$result()
```

The application file must already be available through an ABFS or ABFSS
path. Upload it with
[`fabric_onelake_upload()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_onelake_files.md)
first when necessary. With `wait = FALSE`, the function returns a
`FabricLivyBatch` object immediately; call its `$wait()`, `$result()`,
or `$logs()` methods later.

## Use an Environment for repeatable configuration

A published Fabric Environment can hold Spark settings and libraries
shared by several runs. Discover it and pass its ID when the workload
depends on that configuration:

``` r

environment <- fabric_environments(workspace)[[1L]]

result <- fabric_livy_query(
  lakehouse,
  kind = "pyspark",
  code = "print(spark.version)",
  environment_id = environment$id
)
```

Prefer an Environment over repeating a long set of configuration
overrides in every R call. Leave driver memory, executor memory, cores,
and executor counts at Fabric defaults until the workload has been
measured and sized.

## Decide whether the result belongs in R

Small summaries and samples are good return values for a Livy statement.
A large transformed dataset should normally be written by Spark to a
Lakehouse table or file and then read selectively from R. This keeps
network transfer and local memory use bounded and makes the Fabric
result reusable by other tools.
