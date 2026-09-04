# Discover Fabric User Data Functions

**\[experimental\]**

## Usage

``` r
fabric_user_data_functions(workspace, detail = FALSE, ...)
```

## Arguments

- workspace:

  Workspace name, ID, or object returned by
  [`fabric_workspaces()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_workspaces.md).
  A name is convenient for interactive use; an object avoids an extra
  lookup

- detail:

  Whether to retrieve workload-specific details. Defaults to `FALSE` so
  service-principal and managed-identity callers can use Core item
  discovery.

- ...:

  Authentication and API arguments forwarded to
  [`fabric_items()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_items.md).
  Do not supply `type`; this helper fixes it to `"UserDataFunction"`.

## Value

A list of
[FabricItem](https://kennispunttwente.github.io/fabricQueryR/reference/FabricItem.md)
objects for matching User Data Function items.

## Details

Finds User Data Function items in a workspace. The default
`detail = FALSE` path uses Core item discovery and works with delegated
users, service principals, and managed identities. Set `detail = TRUE`
to call the workload-specific Get API, which currently supports
delegated users only.

This helper is experimental because the package can verify only
lightweight Core discovery through its service-principal development
sandbox. Fabric's User Data Function create, update-definition, detailed
Get, and delete APIs do not currently support service principals or
managed identities, so the sandbox cannot provision and fully inspect a
disposable User Data Function fixture for repeatable end-to-end
coverage.

## References

[List User Data
Functions](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/list-user-data-functions)

[Get User Data
Function](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/get-user-data-function)

[Create User Data
Function](https://learn.microsoft.com/en-us/rest/api/fabric/userdatafunction/items/create-user-data-function)

## Examples

``` r
if (FALSE) { # \dontrun{
workspace <- fabric_workspaces()[[1L]]
functions <- fabric_user_data_functions(workspace)
} # }
```
