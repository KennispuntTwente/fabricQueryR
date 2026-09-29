## CRAN additional issue: noLD

This update addresses the `noLD` test failure reported for version 1.0.0.
The GraphQL row-order test converted decimal text back to a double with
`as.numeric()` and required exact equality with `pi`. With R configured using
`--disable-long-double`, this conversion differs by one unit in the last place.

The test now uses the existing `numeric_test_decode()` JSON decoder, consistently
with the neighboring numeric round-trip tests. Exact equality, row-order
invariance, and preservation of large integer strings and missing values remain
tested. No package runtime behavior has changed.

## R CMD check results

Checked on Windows 11 x64 with R 4.5.1, using `NOT_CRAN=false` and including
vignettes and the PDF manual.

0 errors | 0 warnings | 1 note

The note is environmental: "unable to verify current time".

The CRAN test subset passed 6,775 expectations with 58 expected skips. The focused
GraphQL and numeric-format tests passed all 290 expectations. The full offline
suite also passed; 181 live-service, optional-runtime, and platform-specific tests
were skipped. The local R build has long-double support; the exact CRAN
`--disable-long-double` configuration was not available locally.

## Spelling

"workspaces" is the Microsoft Fabric term for containers of related items and
is spelled correctly.

## Optional dependency

The optional ADBC backend uses `adbi`, which is currently archived on CRAN.
`Additional_repositories` declares https://r-dbi.r-universe.dev, where the
released version 0.1.2 is available. The incoming check confirms its
availability. Tests requiring it are skipped on CRAN even when it is installed,
and skip locally when it is unavailable.

## Reverse dependencies

There are no CRAN reverse dependencies in Depends, Imports, Suggests, or
LinkingTo as of 2026-09-23, so no reverse-dependency checks were needed.
