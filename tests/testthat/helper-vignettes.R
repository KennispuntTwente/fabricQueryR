vignette_r_chunks <- function(path) {
  lines <- readLines(path, warn = FALSE)
  starts <- grep("^```\\{r(?:[ ,].*)?\\}$", lines)
  lapply(starts, function(start) {
    following <- which(seq_along(lines) > start & lines == "```")
    stopifnot(length(following) > 0L)
    end <- following[[1L]]
    list(
      header = lines[[start]],
      body = lines[seq.int(start + 1L, end - 1L)]
    )
  })
}

vignette_safe_setup <- function(chunk) {
  code <- trimws(paste(chunk$body, collapse = "\n"))
  grepl("^knitr::opts_chunk\\$set\\(", code) &&
    !inherits(try(parse(text = code), silent = TRUE), "try-error")
}

documentation_calls <- function(value, free_pattern) {
  calls <- list()
  visit <- function(node) {
    if (is.call(node)) {
      head <- node[[1L]]
      if (is.symbol(head)) {
        name <- as.character(head)
        if (grepl(free_pattern, name)) {
          calls[[length(calls) + 1L]] <<- list(
            kind = "function",
            name = name,
            call = node
          )
        }
      } else if (
        is.call(head) &&
          identical(head[[1L]], quote(`$`)) &&
          length(head) == 3L &&
          is.symbol(head[[3L]])
      ) {
        calls[[length(calls) + 1L]] <<- list(
          kind = "method",
          name = as.character(head[[3L]]),
          call = node
        )
      }
      lapply(as.list(node), visit)
    } else if (is.expression(node) || is.list(node)) {
      lapply(node, visit)
    }
    invisible(NULL)
  }
  visit(value)
  calls
}

documentation_r6_method_registry <- function() {
  exports <- getNamespaceExports("fabricQueryR")
  namespace <- asNamespace("fabricQueryR")
  values <- mget(exports, envir = namespace, inherits = FALSE)
  generators <- Filter(
    function(value) inherits(value, "R6ClassGenerator"),
    values
  )
  registry <- list()

  add_generator <- function(generator, class_name) {
    seen <- character()
    current <- generator
    while (inherits(current, "R6ClassGenerator")) {
      methods <- current[["public_methods"]]
      for (method_name in setdiff(names(methods), seen)) {
        method <- methods[[method_name]]
        symbols <- all.names(body(method), functions = TRUE, unique = TRUE)
        targets <- intersect(symbols, exports)
        targets <- targets[vapply(
          targets,
          function(name) is.function(getExportedValue("fabricQueryR", name)),
          logical(1)
        )]
        if ("ingestion_call" %in% symbols) {
          targets <- union(targets, "fabric_kql_ingestion_status")
        }
        target_parameters <- unlist(lapply(
          targets,
          function(name) names(formals(getExportedValue("fabricQueryR", name)))
        ))
        method_parameters <- names(formals(method))
        registry[[method_name]] <<- c(
          registry[[method_name]],
          list(list(
            class = class_name,
            parameters = union(method_parameters, target_parameters),
            allow_dots = if (length(targets)) {
              "..." %in% target_parameters
            } else {
              "..." %in% method_parameters
            },
            targets = targets
          ))
        )
      }
      seen <- union(seen, names(methods))
      current <- current[["get_inherit"]]()
    }
    invisible(NULL)
  }

  for (class_name in names(generators)) {
    add_generator(generators[[class_name]], class_name)
  }
  registry
}

documentation_call_arguments <- function(call) {
  arguments <- names(as.list(call)[-1L])
  arguments[nzchar(arguments)]
}

documentation_r6_call_matches <- function(call, signatures) {
  supplied <- documentation_call_arguments(call)
  any(vapply(
    signatures,
    function(signature) {
      isTRUE(signature$allow_dots) ||
        !length(setdiff(supplied, signature$parameters))
    },
    logical(1)
  ))
}

documentation_external_methods <- c("Close", "set")

vignette_mock_r6 <- function(fields = list(), methods = list(), class = NULL) {
  object <- new.env(parent = emptyenv())
  list2env(c(fields, methods), envir = object)
  if (!is.null(class)) {
    class(object) <- c(class, "R6")
  }
  object
}

vignette_evaluate_chunks <- function(
  path,
  indices,
  bindings = list(),
  values = list()
) {
  chunks <- vignette_r_chunks(path)
  stopifnot(
    is.numeric(indices),
    length(indices) > 0L,
    all(indices == as.integer(indices)),
    all(indices >= 1L),
    all(indices <= length(chunks)),
    is.list(bindings),
    is.list(values),
    length(bindings) == 0L || !is.null(names(bindings)),
    length(values) == 0L || !is.null(names(values))
  )
  environment <- new.env(parent = baseenv())
  environment$library <- function(...) invisible(TRUE)
  environment$head <- utils::head
  list2env(values, envir = environment)
  list2env(bindings, envir = environment)

  for (index in indices) {
    code <- paste(chunks[[index]]$body, collapse = "\n")
    eval(parse(text = code), envir = environment)
  }
  environment
}
