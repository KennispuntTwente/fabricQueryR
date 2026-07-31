# Inspect the optional Python Delta runtime

Reports the Python requirements declared by fabricQueryR without
starting Python by default. Set `initialize = TRUE` to initialize the
selected Python environment and report installed runtime versions; this
may create a managed environment and download packages.

## Usage

``` r
fabric_delta_config(initialize = FALSE)
```

## Arguments

- initialize:

  Whether to initialize Python.

## Value

A list describing initialization state, requirements, the selected
interpreter, module availability, and installed package versions when
initialized.
