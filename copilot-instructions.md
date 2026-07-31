# fabricQueryR development instructions

## Local Fabric credentials

Local development may use these environment variables:

- `FABRICQUERYR_TENANT_ID`
- `FABRICQUERYR_CLIENT_ID`
- `FABRICQUERYR_CLIENT_SECRET`

Treat their values as secrets. Never print them, include them in command
output, write them to repository files, commit them, or place them in
test snapshots. It is safe to refer to the variable names in code and
documentation.

When all three variables are present, the Python sandbox commands use an
Azure client-secret credential for local `deploy`, `seed`, and
`discover` operations:

``` text
uv --directory tools/fabric-sandbox sync --locked
uv --directory tools/fabric-sandbox run fabric-sandbox doctor
uv --directory tools/fabric-sandbox run fabric-sandbox deploy
uv --directory tools/fabric-sandbox run fabric-sandbox seed
uv --directory tools/fabric-sandbox run fabric-sandbox discover
```

The target workspace/item environment variables still need to identify
the intended development sandbox. Do not rebuild, remove, or clean up
Fabric resources unless the task explicitly authorizes that state
change.

For the persistent local R integration suite, run from the repository
root:

``` r

source("tools/fabric-sandbox/local-integration.R")
run_fabric_integration_tests()
```

The runner first reuses matching cached AzureAuth tokens. If a matching
token is unavailable and the three `FABRICQUERYR_*` variables are
configured, it uses AzureAuth’s client-credentials flow. Its final
fallback is AzureAuth’s normal interactive sign-in. The application or
signed-in user must already have access to the target Fabric workspace.
