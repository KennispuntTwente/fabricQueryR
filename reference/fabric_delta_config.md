# Inspect the optional Python Delta runtime

Shows whether the optional Python tools used for direct Delta reads are
ready. By default this does not start Python. Set `initialize = TRUE` to
prepare the environment and report installed versions; packages may be
downloaded the first time.

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
