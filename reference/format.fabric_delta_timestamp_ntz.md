# Format a Delta timestamp without time zone as wall-clock text

Format a Delta timestamp without time zone as wall-clock text

## Usage

``` r
# S3 method for class 'fabric_delta_timestamp_ntz'
format(x, format = NULL, ...)
```

## Arguments

- x:

  A Delta `timestamp_ntz` vector.

- format:

  Optional output format.

- ...:

  Additional arguments passed to
  [`base::format.POSIXct()`](https://rdrr.io/r/base/strptime.html).

## Value

Character wall-clock timestamps.
