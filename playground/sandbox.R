# Connect an interactive R session to the persistent Fabric sandbox
#
# Run this file from the repository root, then call:
# sandbox <- connect_playground_sandbox()

.playground_find_repository <- function(start = getwd()) {
  current <- normalizePath(start, winslash = "/", mustWork = TRUE)
  repeat {
    description <- file.path(current, "DESCRIPTION")
    package <- if (file.exists(description)) {
      tryCatch(
        read.dcf(description, fields = "Package")[[1L]],
        error = function(error) ""
      )
    } else {
      ""
    }
    if (identical(package, "fabricQueryR")) {
      return(current)
    }

    parent <- dirname(current)
    if (identical(parent, current)) {
      cli::cli_abort("Could not locate the {.pkg fabricQueryR} repository")
    }
    current <- parent
  }
}

.playground_script <- tryCatch(
  normalizePath(
    sys.frame(1)$ofile,
    winslash = "/",
    mustWork = TRUE
  ),
  error = function(error) ""
)

.playground_repository <- if (nzchar(.playground_script)) {
  .playground_find_repository(dirname(.playground_script))
} else {
  .playground_find_repository()
}

source(
  file.path(
    .playground_repository,
    "tools",
    "fabric-sandbox",
    "local-integration.R"
  ),
  local = TRUE
)

# Read an item's display name without depending on package-internal helpers
playground_item_display_name <- function(item) {
  value <- item[["displayName"]]
  if (is.null(value)) {
    value <- item[["display_name"]]
  }
  if (!is.character(value) || length(value) != 1L || is.na(value)) {
    return("")
  }
  value
}

# Add the authentication appropriate to a Livy call without exposing secrets
playground_livy_call <- function(fun, arguments, authentication) {
  overlap <- intersect(names(arguments), names(authentication))
  if (length(overlap)) {
    cli::cli_abort(c(
      "Livy authentication is managed by the playground sandbox",
      "x" = "Remove {length(overlap)} authentication argument{?s}: {.arg {overlap}}"
    ))
  }
  do.call(fun, c(arguments, authentication))
}

# Connect to the marked persistent workspace and discover its seeded targets
connect_playground_sandbox <- function(
  workspace_name = "fabricqueryr-dev-dhrkoning",
  expected_user_id = "9b7dcb13-8485-4429-8b4f-7f1f6ce6ebf5",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID"),
  client_secret = Sys.getenv("FABRICQUERYR_CLIENT_SECRET"),
  auth_args = list()
) {
  required <- c("AzureAuth", "devtools", "jsonlite")
  missing <- required[
    !vapply(required, requireNamespace, logical(1), quietly = TRUE)
  ]
  if (length(missing)) {
    cli::cli_abort(
      "Install {length(missing)} missing playground package{?s}: {.pkg {missing}}"
    )
  }

  if (
    !is.character(workspace_name) ||
      length(workspace_name) != 1L ||
      is.na(workspace_name) ||
      !nzchar(workspace_name)
  ) {
    cli::cli_abort("{.arg workspace_name} must be one non-empty string")
  }
  if (
    !is.null(expected_user_id) &&
      (!is.character(expected_user_id) ||
        length(expected_user_id) != 1L ||
        is.na(expected_user_id) ||
        !nzchar(expected_user_id))
  ) {
    cli::cli_abort(
      "{.arg expected_user_id} must be NULL or one non-empty string"
    )
  }

  fabric_local_validate_secret_identity(
    tenant_id,
    client_id,
    client_secret,
    auth_args
  )
  context <- fabric_local_auth_context(tenant_id, client_id)
  auth_args <- fabric_local_resolve_auth_args(
    auth_args,
    client_id = context$client_id,
    client_secret = client_secret
  )

  devtools::load_all(.playground_repository, quiet = TRUE)
  tokens <- fabric_local_acquire_tokens(
    context$tenant_id,
    context$client_id,
    auth_args
  )
  token <- fabric_local_token_provider(tokens)

  if (!is.null(expected_user_id)) {
    claims <- fabric_local_jwt_claims(
      token("https://api.fabric.microsoft.com/.default")
    )
    fabric_local_validate_identity(
      claims,
      tenant_id = context$tenant_id,
      client_id = context$client_id,
      expected_user_id = expected_user_id,
      auth_args = auth_args
    )
  }

  workspaces <- fabric_workspaces(
    roles = "Admin",
    prefer_workspace_endpoints = TRUE,
    token = token
  )
  matches <- Filter(
    function(workspace) {
      identical(workspace[["displayName"]], workspace_name)
    },
    workspaces
  )
  if (length(matches) != 1L) {
    cli::cli_abort(c(
      "Could not resolve the persistent Fabric workspace",
      "x" = paste0(
        "Expected one workspace named {.val {workspace_name}} but found ",
        "{length(matches)}"
      ),
      "i" = paste0(
        "Run the {.strong Manage persistent Fabric sandbox} workflow with ",
        "{.code action = rebuild}"
      )
    ))
  }

  workspace <- matches[[1L]]
  marker <- workspace[["description"]]
  if (
    !is.character(marker) ||
      length(marker) != 1L ||
      !startsWith(marker, "fabricqueryr-persistent;")
  ) {
    cli::cli_abort(
      "The workspace does not carry the persistent sandbox ownership marker"
    )
  }

  items <- fabric_items(
    workspace,
    detail = TRUE,
    detail_errors = "record",
    token = token
  )
  item_names <- vapply(items, playground_item_display_name, character(1))
  if (!all(nzchar(item_names)) || anyDuplicated(item_names)) {
    cli::cli_abort(
      "The persistent sandbox returned missing or duplicate item names"
    )
  }
  names(items) <- item_names

  target_names <- c(
    lakehouse = "TestLakehouse",
    lakehouse_no_schemas = "TestLakehouseNoSchemas",
    warehouse = "TestWarehouse",
    warehouse_snapshot = "TestWarehouseSnapshot",
    sql_database = "TestSQLDatabase",
    mirrored_database = "TestMirroredDatabase",
    eventhouse = "TestEventhouse",
    kql_database = "TestKQLDatabase",
    semantic_model = "FabricQueryRIntegrationModel",
    arrow_semantic_model = "FabricQueryRArrowIntegrationModel",
    graphql_api = "TestGraphQL",
    seed_notebook = "SeedFixtures",
    job_notebook = "JobFixtures",
    pipeline = "TestPipeline",
    spark_job = "TestSparkJob"
  )
  missing_targets <- setdiff(unname(target_names), names(items))
  if (length(missing_targets)) {
    cli::cli_abort(c(
      "The persistent sandbox is missing required seeded items",
      "x" = "Missing {length(missing_targets)} item{?s}: {.val {missing_targets}}",
      "i" = "Rebuild it from the current branch before using the playground"
    ))
  }
  targets <- items[unname(target_names)]
  names(targets) <- names(target_names)

  livy_authentication <- if (fabric_local_uses_client_credentials(auth_args)) {
    list(
      token = token,
      audience = "https://analysis.windows.net/powerbi/api/.default"
    )
  } else {
    list(
      tenant_id = context$tenant_id,
      client_id = context$client_id,
      auth_args = auth_args
    )
  }
  livy <- list(
    query = function(...) {
      playground_livy_call(
        fabric_livy_query,
        list(...),
        livy_authentication
      )
    },
    session = function(...) {
      playground_livy_call(
        fabric_livy_session,
        list(...),
        livy_authentication
      )
    },
    batch = function(...) {
      playground_livy_call(
        fabric_livy_batch_submit,
        list(...),
        livy_authentication
      )
    }
  )

  invisible(structure(
    list(
      workspace = workspace,
      items = items,
      targets = targets,
      token = token,
      livy = livy,
      tenant_id = context$tenant_id,
      client_id = context$client_id
    ),
    class = c("fabricqueryr_playground_sandbox", "list")
  ))
}
