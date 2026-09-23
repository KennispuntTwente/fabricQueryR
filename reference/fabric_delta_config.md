# Inspect the optional Python Delta runtime

Shows whether the optional Python tools used for direct Delta reads are
ready By default this does not start Python. Set `initialize = TRUE` to
prepare the environment, check the minimum Python version and required
module imports, and report installed versions. An unusable runtime
raises an error with setup instructions. Use `initialize = FALSE` to
inspect it without this check

## Usage

``` r
fabric_delta_config(initialize = FALSE)
```

## Arguments

- initialize:

  Whether to initialize Python

## Value

A list describing initialization state, requirements, the selected
interpreter, module availability, and installed package versions when
initialized. `initialized` only indicates whether Python has started; it
does not mean the Delta dependencies are available

## Use a managed environment (recommended)

For automatic installation, explicitly select a 'reticulate'-managed
environment. First restart R (in RStudio: Session \> Restart R), then
run:

    Sys.setenv(RETICULATE_PYTHON = "managed")
    library(fabricQueryR)
    fabric_delta_config(initialize = TRUE)

`"managed"` is a special setting, not an environment name or a Python
path. The first line tells 'reticulate' to create or reuse a suitable
environment. Loading 'fabricQueryR' declares the required Python version
and packages through
[`reticulate::py_require()`](https://rstudio.github.io/reticulate/reference/py_require.html).
The final line starts Python, triggering 'uv' to download the runtime
and dependencies if needed, and checks setup. 'reticulate' also
downloads 'uv' if needed; no separate `py_install()` call is necessary

The restart is required if Python has already started: setting
`RETICULATE_PYTHON` cannot switch the interpreter in a running Python
session.

To keep this selection for future sessions in this project, add the line
`RETICULATE_PYTHON=managed` to the project's `.Renviron` file, then
restart R. Otherwise, repeat the
[`Sys.setenv()`](https://rdrr.io/r/base/Sys.setenv.html) line at the
start of each R session

Without this explicit selection, a Python chosen through RStudio,
environment variables,
[`reticulate::use_python()`](https://rstudio.github.io/reticulate/reference/use_python.html),
or a project virtualenv can take precedence over the managed
environment. `py_require()` declares requirements but does not install
them into that existing Python, and `initialize = TRUE` does not change
that choice. For example, selecting Python 3.9 leaves the Delta
requirements unmet even though Python has started.
[`reticulate::py_config()`](https://rstudio.github.io/reticulate/reference/py_config.html)
reports the selected interpreter and why it was chosen. After the setup
above succeeds, both entries in `available` should be `TRUE` and
`versions` should report the installed Delta packages

## Create a reusable environment with reticulate

For an environment you manage yourself, restart R and create a
virtualenv from an installed Python 3.11. Then install the required
packages into that named environment and select its interpreter before
initializing Python:

    reticulate::virtualenv_create("fabricQueryR", version = "3.11")
    reticulate::py_install(
      c("deltalake==1.6.2", "nanoarrow==0.8.0"),
      envname = "fabricQueryR",
      method = "virtualenv"
    )
    Sys.setenv(
      RETICULATE_PYTHON = reticulate::virtualenv_python("fabricQueryR")
    )
    library(fabricQueryR)
    fabric_delta_config(initialize = TRUE)

`virtualenv_create()` needs a compatible installed Python and reuses an
existing environment of that name without upgrading its Python. If
needed, install Python first with
`reticulate::install_python("3.11:latest")` or use the 'uv' alternative
below. `install_python()` uses 'pyenv' / 'pyenv-win'; it is separate
from the automatic 'uv' setup described above

`py_install()` installs into the named virtualenv using 'pip'. Always
pass `envname` to make the destination explicit. For an existing Conda
environment, use `method = "conda", pip = TRUE` with that environment's
name instead

In later R sessions, repeat the
[`Sys.setenv()`](https://rdrr.io/r/base/Sys.setenv.html) selection
before using Python; installation is needed only when creating or
updating the environment.
[`reticulate::use_virtualenv()`](https://rstudio.github.io/reticulate/reference/use_python.html)
is another way to select it, but an existing `RETICULATE_PYTHON` setting
takes precedence over that selection

## Create a reusable environment with uv

If the 'uv' command-line tool is already installed and available on
`PATH`, it can create a new project environment and download Python if
necessary. From the project directory, run in a terminal:

    uv venv --python 3.11 --seed .venv-fabricQueryR

`--seed` installs 'pip' so that
[`reticulate::py_install()`](https://rstudio.github.io/reticulate/reference/py_install.html)
can install into the environment. In a fresh R session in the same
project directory:

    reticulate::py_install(
      c("numpy", "deltalake==1.6.2", "nanoarrow==0.8.0"),
      envname = "./.venv-fabricQueryR",
      method = "virtualenv"
    )
    Sys.setenv(
      RETICULATE_PYTHON = reticulate::virtualenv_python("./.venv-fabricQueryR")
    )
    library(fabricQueryR)
    fabric_delta_config(initialize = TRUE)

The `./` makes this a project path rather than a named environment under
the virtualenv directory used by 'reticulate'. 'numpy' is included
because 'uv' does not install it automatically

This environment is managed by you; 'reticulate' does not resolve
`py_require()` declarations into it. Use `py_install()` with that
explicit `envname`, or `uv pip install --python .venv-fabricQueryR ...`,
to update it. See the [uv environment
guide](https://docs.astral.sh/uv/pip/environments/) for details

## Install into an existing standalone Python

Select Python 3.10 or newer before initialization. Install the Python
dependencies into that exact interpreter, for example from R:

    system2("/path/to/python", c(
      "-m", "pip", "install", "deltalake==1.6.2", "nanoarrow==0.8.0"
    ))

On Windows, use the full path to `python.exe` with forward slashes.
Restart R, select that interpreter with
[`reticulate::use_python()`](https://rstudio.github.io/reticulate/reference/use_python.html),
then run `fabric_delta_config(initialize = TRUE)` to verify setup

## Examples

``` r
# Inspect requirements without starting Python or downloading anything
config <- fabric_delta_config()
config[c("initialized", "requirements", "available")]
#> $initialized
#> [1] FALSE
#> 
#> $requirements
#> $requirements$python_version
#> [1] ">=3.10"
#> 
#> $requirements$packages
#> [1] "numpy"            "deltalake==1.6.2" "nanoarrow==0.8.0"
#> 
#> 
#> $available
#> deltalake nanoarrow 
#>        NA        NA 
#> 
```
