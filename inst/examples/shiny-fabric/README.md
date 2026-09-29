# Fabric SQL with each visitor's Microsoft sign-in

Install fabricQueryR, Shiny, the target-enabled shinyOAuth version, DBI, odbc,
and Microsoft ODBC Driver 18 for SQL Server. During development install the
sibling shinyOAuth checkout with `R CMD INSTALL ../shinyoauth` first.

Configure these environment variables on the server:

| Variable | Value |
| --- | --- |
| `ENTRA_TENANT_ID` | Fixed directory GUID containing the Fabric data |
| `ENTRA_CLIENT_ID` | Web app registration's application ID |
| `ENTRA_CLIENT_SECRET` | Server-held app secret |
| `ENTRA_REDIRECT_URI` | Registered callback, default `http://localhost:8100/` |
| `FABRIC_SQL_SERVER` | Warehouse or SQL endpoint hostname |
| `FABRIC_SQL_DATABASE` | Database/catalog name |

Register that callback as a **Web** redirect URI and configure the delegated
Azure SQL Database `user_impersonation` permission. Complete any consent your
tenant requires. Users must also have permission to connect to the database.
The secret authenticates the app; SQL queries still use each signed-in user.

From this directory run:

```r
shiny::runApp(".", host = "127.0.0.1", port = 8100)
```

Open `http://localhost:8100/` exactly. The app lists tables visible through the
user's SQL metadata permissions and uses a bound parameter for the name filter.
Zero rows can be a valid result. It does not create or modify any tables.

For deployment, use a registered HTTPS callback and one R process with correct
session routing. See `vignette("shiny-integration")` for discovery, GraphQL,
existing shinyOAuth apps and the pending restricted-user acceptance checklist.
