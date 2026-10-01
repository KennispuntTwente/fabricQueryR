# Wrap a Shiny UI for Microsoft Fabric sign-in

Uses 'shinyOAuth' callback routing with the client from
[`fabric_shiny_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_config.md).
Add your own sign-in, sign-out and retry buttons; call the matching
methods returned by
[`fabric_shiny_server()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_server.md).

## Usage

``` r
fabric_shiny_ui(ui, id, config)
```

## Arguments

- ui:

  A Shiny UI object or function accepted by
  [`shinyOAuth::oauth_ui()`](https://lukakoning.github.io/shinyOAuth/reference/oauth_ui.html).

- id:

  Module identifier, matching
  [`fabric_shiny_server()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_server.md).

- config:

  Configuration returned by
  [`fabric_shiny_config()`](https://kennispunttwente.github.io/fabricQueryR/reference/fabric_shiny_config.md).

## Value

The UI function returned by
[`shinyOAuth::oauth_ui()`](https://lukakoning.github.io/shinyOAuth/reference/oauth_ui.html).
Use `shiny::shinyApp(ui, server, uiPattern = ".*")` to route callback
paths.
