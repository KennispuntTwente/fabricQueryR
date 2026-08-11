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

## Roxygen2 documentation

Write public documentation for a typical R user first. Assume the reader
knows R, but may not know Fabric’s REST APIs, Azure terminology, storage
protocols, token audiences, or internal response formats.

- Start each topic with the user task and the result, in plain language.
- Put the simplest and most common workflow before advanced
  alternatives.
- Prefer Fabric portal terms that users can see, and briefly explain an
  unavoidable specialist term the first time it appears.
- Keep parameter descriptions practical: say what to pass, when it is
  needed, and what the default does. Avoid repeating implementation
  details there.
- Move permissions, service limits, protocol behavior, exact type
  mappings, retry rules, and unusual authentication flows into clearly
  named sections after the basic usage guidance.
- Keep technical detail when it affects correctness, security, data
  loss, precision, performance, or troubleshooting, but do not make it
  the opening explanation.
- Use examples based on discovered Fabric records when that is the
  easiest workflow; show raw IDs, endpoints, or connection strings as
  alternatives.
- Before adding detail, ask whether it helps most users choose, call, or
  understand the function. If it serves maintainers rather than users,
  prefer an internal comment or developer documentation.

When changing a public function, review its whole help topic for
readability; do not append new implementation notes to an already dense
introduction.
