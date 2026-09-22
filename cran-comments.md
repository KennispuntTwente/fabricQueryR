## R CMD check results

0 errors | 0 warnings

Tests, examples, vignettes, and both manuals pass. Examples and integration
tests that require Microsoft Fabric credentials are guarded and do not
authenticate or contact Fabric during CRAN checks.

## Optional dependency

The optional ADBC backend uses `adbi`, which is currently archived on CRAN.
`Additional_repositories` declares https://r-dbi.r-universe.dev, where the
released version 0.1.2 is available. The incoming check confirms its
availability. Tests requiring it are skipped on CRAN even when it is installed,
and skip locally when it is unavailable.

## Tests

CRAN runs the deterministic unit-test subset. Tests requiring live Fabric,
Python fixtures, repository tooling, real-time polling, tight deadlines, or
local HTTP servers are reserved for development and CI. GitHub Actions runs
the full unit suite with `NOT_CRAN=true`; `devtools::test()` also runs the full
unit suite. Live Fabric and Python integration tests retain their separate
opt-in workflows.

## Reverse dependencies

There are no CRAN reverse dependencies in Depends, Imports, Suggests, or
LinkingTo as of 2026-09-22, so no reverse-dependency checks were needed.
