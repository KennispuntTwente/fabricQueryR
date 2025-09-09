
<!-- README.md is generated from README.Rmd. Please edit that file -->

# fabricQueryR

<!-- badges: start -->

[![R-CMD-check](https://github.com/kennispunttwente/fabricQueryR/actions/workflows/R-CMD-check.yaml/badge.svg)](https://github.com/kennispunttwente/fabricQueryR/actions/workflows/R-CMD-check.yaml)
[![CRAN
status](https://www.r-pkg.org/badges/version/fabricQueryR)](https://CRAN.R-project.org/package=fabricQueryR)
<!-- badges: end -->

‘fabricQueryR’ is an R package which helps you to query data from
Microsoft Fabric in R. It comes with three methods which help you to get
your Microsoft Fabric data into R:

1.  Create a connection to a SQL endpoint (e.g., from a `Lakehouse` or
    `Data Warehouse` item): `fabric_sql_connect()`. This results in a
    ‘DBI’ connection object which you can execute SQL queries with,
    and/or use with ‘DBI’-compatible packages like ‘dbplyr’.

2.  Execute a DAX query against a Fabric/Power Bi `Semantic Model` item:
    `fabric_pbi_dax_query()`. With this, you can run DAX queries against
    a Fabric/Power Bi dataset and get the results as a ‘tibble’
    dataframe.

3.  Read a Delta table from a Fabric `Lakehouse` item:
    `fabric_onelake_read_delta_table()`. This function downloads the
    underlying Parquet files from the Delta table stored in OneLake
    (ADLS Gen2) and returns the data as a ‘tibble’ dataframe.

## Installation

You can install the development version of ‘fabricQueryR’ like so:

``` r
if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes")
}

remotes::install_github("kennispunttwente/fabricQueryR")
```

Or, install the latest release from CRAN:

``` r
install.packages("fabricQueryR")
```

## Usage

See the
[reference](https://kennispunttwente.github.io/fabricQueryR/reference/index.html)
for the full documentation of all functions.

Below is a code snippet showing how to use the three methods to get data
from Fabric into R:

``` r

# First find your 'tenant' ID & 'client' ID (app registration) in Azure/Entra
# You may be able to use the default Azure CLI app id;
#   this will be automatically used if you do not set 'FABRICQUERYR_CLIENT_ID'
# The AzureAuth package is used to acquire tokens; you may be redirected
#   to a browser window to sign in the first time

# Sys.setenv(FABRICQUERYR_TENANT_ID = "...")
# Sys.setenv(FABRICQUERYR_CLIENT_ID = "...")

# SQL connection to Data Warehouse or Lakehouse --------------------------------

# Find your SQL connection string in Fabric by going to a Lakehouse or Data
#   Warehouse item; then Settings -> SQL analytics endpoint
# Ensure that the account/principal you authenticate with has access to
#   the workspace 

# Get connection
con <- fabric_sql_connect(
  server = "2gxz...4qiy.datawarehouse.fabric.microsoft.com"
)

# List databases
DBI::dbGetQuery(con, "SELECT name FROM sys.databases")

# List tables in the current database
DBI::dbGetQuery(
  con,
  "
  SELECT TABLE_SCHEMA, TABLE_NAME
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_TYPE = 'BASE TABLE'
  "
)

# Read 'Customers' table
df_sql <- DBI::dbReadTable(con, "Customers")

# Close connection
DBI::dbDisconnect(con)


# Table from Lakehouse via OneLake data access ---------------------------------

# Ensure that the account/principal you authenticate with has access via
#   being part of the workspace, or via Lakehouse -> Manage OneLake data access

df_onelake <- fabric_onelake_read_delta_table(
  table_path = "Customers",
  workspace_name = "ExampleWorkspace",
  lakehouse_name = "Lakehouse.Lakehouse",
)


# DAX query against Semantic Model ---------------------------------------------

# Ensure that the account you use to authenticate has access to the workspace,
#   or that you have been granted 'Build' permissions on the dataset (via share)

df_dax <- fabric_pbi_dax_query(
  connstr = paste0(
    "Data Source=powerbi://api.powerbi.com/v1.0/myorg/",
    "ExampleWorkspace;Initial Catalog=test data 1;"
  ),
  dax = "EVALUATE TOPN(100000, 'Sheet1')"
)
```

## Background

Microsoft Fabric is a new data platform from Microsoft which combines
various data services, including data warehousing, data lakes, and
business intelligence. It is built on top of Azure Data Services and
integrates with Power BI for analytics and reporting. Microsoft is
actively promoting Fabric as the next-generation data platform for
organizations using Microsoft Azure and Power BI.

As my organization started working with Microsoft Fabric, I found that
that loading data into R from Fabric was not yet straightforward, and
took some effort to get working. To help others in the same situation, I
decided to share the functions I created to make this easier.
