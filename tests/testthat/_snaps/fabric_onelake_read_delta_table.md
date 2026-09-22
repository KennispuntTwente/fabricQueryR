# Delta setup explains how to replace an unsupported Python

    Code
      fabric_delta_config(initialize = TRUE)
    Condition
      Error in `fabric_delta_config()`:
      ! The Python Delta runtime is not ready.
      i Selected Python 3.9: C:/Python39/python.exe
      x Python 3.10 or newer is required.
      x Cannot import required Python modules: deltalake, nanoarrow.
      i Automatic installation requires a reticulate-managed environment; existing Python environments must provide the dependencies themselves.
      i Restart R, then run Sys.setenv(RETICULATE_PYTHON = "managed") before any Python code, followed by fabric_delta_config(initialize = TRUE).
      i See ?fabric_delta_config for setup instructions.

# Delta setup gives an installation command for a supported Python

    Code
      fabric_delta_config(initialize = TRUE)
    Condition
      Error in `fabric_delta_config()`:
      ! The Python Delta runtime is not ready.
      i Selected Python 3.12.7: C:/Custom Python/python.exe
      x Cannot import required Python modules: nanoarrow.
      i Automatic installation requires a reticulate-managed environment; existing Python environments must provide the dependencies themselves.
      i Restart R, then run Sys.setenv(RETICULATE_PYTHON = "managed") before any Python code, followed by fabric_delta_config(initialize = TRUE).
      i To keep this Python instead, run in R: system2("C:/Custom Python/python.exe", c("-m", "pip", "install", "deltalake==1.6.2", "nanoarrow==0.8.0"))
      i Then restart R and run fabric_delta_config(initialize = TRUE) again.
      i See ?fabric_delta_config for setup instructions.

# Python initialization failures include setup guidance

    Code
      fabric_delta_config(initialize = TRUE)
    Condition
      Error in `fabric_delta_abort_python()`:
      ! Unable to initialize the Python Delta runtime.
      x Python installation not found.
      i Automatic installation requires a reticulate-managed environment; existing Python environments must provide the dependencies themselves.
      i Restart R, then run Sys.setenv(RETICULATE_PYTHON = "managed") before any Python code, followed by fabric_delta_config(initialize = TRUE).
      i See ?fabric_delta_config for setup instructions.

# Delta reads reject missing modules before calling Python APIs

    Code
      rlang::cnd_signal(error)
    Condition
      Error in `fabric_delta_config()`:
      ! The Python Delta runtime is not ready.
      i Selected Python 3.12: /custom/python
      x Cannot import required Python modules: deltalake.
      i Automatic installation requires a reticulate-managed environment; existing Python environments must provide the dependencies themselves.
      i Restart R, then run Sys.setenv(RETICULATE_PYTHON = "managed") before any Python code, followed by fabric_delta_config(initialize = TRUE).
      i To keep this Python instead, run in R: system2("/custom/python", c("-m", "pip", "install", "deltalake==1.6.2", "nanoarrow==0.8.0"))
      i Then restart R and run fabric_delta_config(initialize = TRUE) again.
      i See ?fabric_delta_config for setup instructions.

