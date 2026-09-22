# Test suites

`devtools::test()` runs the full unit suite. The check, coverage, and Fabric
integration workflows explicitly set `NOT_CRAN=true`. Live Fabric and Python
runtime tests still require their existing opt-in environment and fixtures.

Test setup disables cli progress rendering for the duration of the test run,
including `devtools::test()` and package checks. Polling progress still runs,
while messages, warnings, errors, and test diagnostics remain visible. The
caller's progress settings are restored afterwards. Tests specifically checking
rendered progress can override `cli.progress_handlers_only` locally.

With `NOT_CRAN` unset or set to `false`, `tests/testthat.R` excludes live Fabric
integration, Python runtime fixtures, and repository/development-tool tests.
Ordinary unit tests remain selected, including SQL parsing and binding, numeric
conversion, request construction, and polling with simulated clocks.

Individual tests use `skip_on_cran()` when they require the non-CRAN `adbi`
package, perform real retry or polling delays, assert short wall-clock deadlines,
start a local HTTP subprocess, or stress-test thousands of random inputs. Keep
these guards before package loading or other setup. Mocked ADBC tests that do
not require `adbi` remain in the CRAN subset.

To exercise the CRAN subset in the built package, use:

```r
devtools::check(env_vars = c(NOT_CRAN = "false"))
```

Setting `NOT_CRAN=false` before `devtools::test()` does not select this subset:
devtools deliberately enables non-CRAN testing. Use the package check entry
point when validating CRAN behavior, and test with `adbi` absent as well as
installed. New slow or timing-sensitive tests should receive a targeted guard;
keep small deterministic examples of the same behavior on CRAN.

The Linux release CI job also runs the CRAN subset in an isolated library that
excludes `adbi`, after its full-suite check. It fails if `adbi` can be found or is
loaded during the subset run.
