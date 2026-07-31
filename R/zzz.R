.delta_python <- new.env(parent = emptyenv())

.onLoad <- function(libname, pkgname) {
  reticulate::py_require(
    packages = c(
      "deltalake>=1.6.2",
      "nanoarrow>=0.8.0"
    ),
    python_version = ">=3.10"
  )
  .delta_python$deltalake <- reticulate::import(
    "deltalake",
    delay_load = TRUE,
    convert = FALSE
  )
  .delta_python$nanoarrow <- reticulate::import(
    "nanoarrow",
    delay_load = TRUE,
    convert = FALSE
  )
  .delta_python$builtins <- reticulate::import_builtins(
    delay_load = TRUE,
    convert = FALSE
  )
}
