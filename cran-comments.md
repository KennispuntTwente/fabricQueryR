## CRAN additional issue: noLD

This update addresses the `noLD` test failure reported for version 1.0.0.
The GraphQL row-order test converted decimal text back to a double with
`as.numeric()` and required exact equality with `pi`. With R configured using
`--disable-long-double`, this conversion differs by one unit in the last place.
This update fixes that test by using the existing `numeric_test_decode()` JSON decoder,
which preserves the original decimal text and avoids the conversion to double.

## R CMD check results

0 errors | 0 warnings | 0 notes

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
LinkingTo as of 2026-09-29, so no reverse-dependency checks were needed.
