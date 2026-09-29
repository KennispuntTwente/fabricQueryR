.delta_python <- new.env(parent = emptyenv())
.fabric_delta_min_python <- "3.10"
.fabric_delta_python_packages <- c("deltalake==1.6.2", "nanoarrow==0.8.0")

# Register the optional Python Delta dependencies when the package loads
# Inputs are standard R package-load names; the function has no return value
.onLoad <- function(libname, pkgname) {
  reticulate::py_require(
    packages = .fabric_delta_python_packages,
    python_version = paste0(">=", .fabric_delta_min_python)
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
