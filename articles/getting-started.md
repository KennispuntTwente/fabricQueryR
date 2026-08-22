# Get started with 'fabricQueryR'

Microsoft Fabric is a collection of services for storing, transforming,
and reporting on data. ‘fabricQueryR’ lets you work with many of those
services from R: you can read Fabric data, send R data to Fabric, and
start work that runs inside Fabric.

This guide introduces the basic Fabric concepts and completes one small
read. Start here if you are new to either Fabric or ‘fabricQueryR’, then
continue to a task-specific vignette.

## The Fabric objects you will see

A *workspace* is a shared area that contains Fabric items. An *item* is
a resource inside a workspace, such as a Lakehouse, Warehouse, semantic
model, or notebook.

The most common data items have different purposes:

| Item | Think of it as | A common R task |
|----|----|----|
| Lakehouse | Files plus managed data tables | Read or write a table or file |
| Warehouse | A relational SQL database | Query or load business tables |
| Eventhouse | A database for event and time-series data | Query with KQL or ingest events |
| Semantic model | Report-ready tables, relationships, and calculations | Query with DAX or refresh the model |
| API for GraphQL | A structured API in front of Fabric data | Request selected fields |

*OneLake* is the storage layer shared by Fabric items. In a Lakehouse,
the `Files/` area contains ordinary files and the `Tables/` area
contains managed Delta tables. Delta is a storage format that supports
efficient reads and writes, schema evolution, and transactional
consistency.

## Sign in

This development guide uses APIs that may not yet be in the CRAN
release. Install the development version, load it, and set your
organization’s Microsoft Entra tenant ID:

``` r

if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes")
}
remotes::install_github("kennispunttwente/fabricQueryR")

library(fabricQueryR)
Sys.setenv(FABRICQUERYR_TENANT_ID = "<your-tenant-id>")

# Optional, if your organization requires an approved application:
Sys.setenv(FABRICQUERYR_CLIENT_ID = "<your-app-client-id>")
```

The first Fabric call may open a browser. Sign in with the same work or
school account that you use in the Fabric portal.

If your organization requires an approved application, your
administrator may also give you a client ID to set as
`FABRICQUERYR_CLIENT_ID`.

The [authentication
vignette](https://kennispunttwente.github.io/fabricQueryR/articles/authentication.md)
explains this setup and the different ways to authenticate in more
detail.

## Find a workspace and an item

Start by listing the workspaces that your account can access:

``` r

# List all workspaces you can access
workspaces <- fabric_workspaces()
```

The result is a list of workspaces you have access to. If the list is
empty, check that your account has been granted access to a workspace in
the Fabric portal. If the list is not empty, select a specific
workspace:

``` r

# Select the first workspace in the list
workspace <- workspaces[[1L]]
workspace$displayName
```

For a script that will run repeatedly, selecting by exact name is more
robust:

``` r

# Select a workspace by name
workspaces <- fabric_workspaces()
matches <- Filter(
  \(x) identical(x$displayName, "Analytics workspace"),
  workspaces
)
stopifnot(length(matches) == 1L)
workspace <- matches[[1L]]
```

Now list all items in the workspace, or ask directly for items of a
specific type:

``` r

# List all items in the workspace
items <- fabric_items(workspace)
items

# List only Lakehouses in the workspace
lakehouses <- fabric_lakehouses(workspace)
lakehouse <- lakehouses[[1L]]
lakehouse$displayName
```

A discovered item contains metadata such as its name and type. Pass that
item directly to other ‘fabricQueryR’ functions instead of copying IDs
or connection details by hand.

## Complete a first read

If the workspace contains a Lakehouse, reading one managed table is a
simple first workflow:

``` r

# List the tables in the Lakehouse
tables <- fabric_lakehouse_tables(lakehouse)
tables[c("schema", "name", "type")]

# Select the first table and read a small number of rows
first_table <- tables[1L, ]
rows <- fabric_lakehouse_read_table(
  lakehouse,
  first_table,
  limit = 100L
)

# Show the first few rows
head(rows)
```

The result is a tibble, which can be used with base R, ‘dplyr’, plotting
packages, or other familiar R tools. `limit = 100L` keeps this first
request small while you confirm that access and table selection are
correct.

## Choose the next guide

There are often several valid ways to move the same data. The vignettes
below compare the options and show how to use them. Continue with one of
the following vignettes:

- [Bring Fabric data into
  R](https://kennispunttwente.github.io/fabricQueryR/articles/reading-data.md)
  compares SQL, Lakehouse, Warehouse, Eventhouse, semantic-model,
  OneLake-file, GraphQL, and Spark reads
- [Bring R data into Microsoft
  Fabric](https://kennispunttwente.github.io/fabricQueryR/articles/ingesting-data.md)
  compares ways to send an R object or an existing file to Fabric
- [Working with Fabric Lakehouses and
  OneLake](https://kennispunttwente.github.io/fabricQueryR/articles/onelake-and-lakehouse.md)
  explains Lakehouse files and tables in more detail
- [Working with Livy
  (Spark)](https://kennispunttwente.github.io/fabricQueryR/articles/spark-with-livy.md)
  introduces remote Spark work after the simpler read and write paths
