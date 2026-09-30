# From the repository root: shiny::runApp("playground/shiny", port = 8100)
source("../sandbox.R")
source("queries.R")
source("application.R")

mode <- playground_shiny_mode()
sandbox <- connect_playground_sandbox(allow_partial = TRUE)
playground_shiny_app(sandbox, .playground_repository, mode = mode)
