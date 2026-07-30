# Localize a Delta timestamp without time zone

Localize a Delta timestamp without time zone

## Usage

``` r
# S3 method for class 'fabric_delta_timestamp_ntz'
as.POSIXct(x, tz = "UTC", ...)
```

## Arguments

- x:

  A Delta `timestamp_ntz` vector.

- tz:

  IANA timezone in which to interpret the wall-clock values.

- ...:

  Unused.

## Value

A `POSIXct` vector localized in `tz`.
