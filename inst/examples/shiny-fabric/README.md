# Fabric data in Shiny with each visitor's Microsoft sign-in

Install fabricQueryR, Shiny (1.8.1 or later), bslib (0.7.0 or later), mirai,
promises (1.3.0 or later), and the target-enabled shinyOAuth version. The app
prefers mirai when installed; without it, install future for the fallback.
During development install the sibling shinyOAuth checkout with
`R CMD INSTALL ../shinyoauth` first.

The app starts with Warehouse and Lakehouse SQL, followed by a DAX sales
dashboard, KQL, OneLake files and Delta tables, and GraphQL. Keep the entries
you need in the `sources` vector in `app.R`. Its sign-in configuration selects those services
automatically.

To start with SQL, keep only the Warehouse entry and configure:

| Variable | Value |
| --- | --- |
| `ENTRA_TENANT_ID` | Fixed directory GUID containing the Fabric data |
| `ENTRA_CLIENT_ID` | Web app registration's application ID |
| `ENTRA_CLIENT_SECRET` | Server-held app secret |
| `ENTRA_REDIRECT_URI` | Registered callback, default `http://localhost:8100/` |
| `FABRIC_WAREHOUSE_SQL_SERVER` | Warehouse SQL endpoint hostname |
| `FABRIC_WAREHOUSE_SQL_DATABASE` | Warehouse database name |

Register that callback as a Web redirect URI and configure the delegated
Azure SQL Database `user_impersonation` permission. Complete any consent your
tenant requires. Users also need permission to connect and read the data.

Copy the connection settings from your Warehouse and replace `dbo.Sales` in
the query with your table. SQL requires DBI, odbc and Microsoft ODBC Driver 18
for SQL Server on the host.

The DAX entry shows how to reuse a semantic model's measure in a Shiny chart,
with a year filter. Set `FABRIC_WORKSPACE_ID` and `FABRIC_SEMANTIC_MODEL_ID`,
and replace `'Product'[Category]`, `'Date'[Year]` and `[Total Sales]` with names
from your model. The registration needs Power BI Service `Dataset.Read.All`;
users need Read and Build permission on the model, and the tenant must enable
the Dataset Execute Queries REST API.

For the other sources, set their environment variables used in `read_data()`
and replace the example table and field names. The vignette explains each
connection and its delegated permission:

```r
vignette("shiny-integration", package = "fabricQueryR")
```

OneLake file reads require arrow; Delta reads require reticulate and the runtime
described by `fabric_delta_config()`. DAX, KQL and GraphQL use HTTP APIs.

From this directory run:

```r
shiny::runApp(".", host = "127.0.0.1", port = 8100)
```

Open `http://localhost:8100/`, sign in and click Load data to run the Warehouse
query. Select another configured source to try its query.
For DAX, choose a year present in the model. Change the year and click Load data
again to update the chart.

Queries use `ExtendedTask` with mirai, or `future_promise()` as a fallback, so
the app stays responsive while data loads. The Load data button stays busy
until its task finishes.
The app starts two background R workers; adjust `daemons(2)` or `workers = 2`
for your host. `onStop()` shuts down the workers when the app stops.

For deployment, use a registered HTTPS callback and correct session routing.
The vignette contains the complete app code and guidance for adapting an
existing shinyOAuth app.
