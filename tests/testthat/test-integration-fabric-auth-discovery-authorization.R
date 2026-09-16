# Fabric integration coverage: auth discovery authorization
test_that("valid restricted identities can read allowed Fabric and OneLake resources only", {
  config <- fabric_test_authorization_matrix()
  for (identity in config$identities) {
    api_token <- fabric_test_identity_token(identity$api_token_env)
    allowed <- fabric_items(identity$allowed_workspace, token = api_token)
    expect_gt(length(allowed), 0L)
    denied <- expect_error(
      fabric_items(identity$denied_workspace, token = api_token),
      class = "fabric_http_error"
    )
    expect_identical(denied$status, 403L)
    storage_token <- fabric_test_identity_token(identity$storage_token_env)
    source <- identity$onelake
    bytes <- fabric_onelake_download(
      source$workspace,
      source$item,
      source$allowed_path,
      token = storage_token
    )
    expect_identical(bytes, charToRaw(enc2utf8(source$expected_text)))
    denied <- expect_error(
      fabric_onelake_download(
        source$workspace,
        source$item,
        source$denied_path,
        token = storage_token
      ),
      class = "fabric_http_error"
    )
    expect_identical(denied$status, 403L)
  }
})

test_that("DAX RLS restricts rows for impersonation, roles and custom data", {
  config <- fabric_test_authorization_matrix()
  for (case in config$dax) {
    token <- fabric_test_identity_token(case$token_env)
    query <- function(api, options = list(), user = NULL) {
      result <- fabric_pbi_dax_query(
        workspace_id = config$workspace,
        dataset_id = config$dataset,
        dax = config$query,
        api = api,
        impersonated_user = user,
        arrow_options = options,
        token = token
      )
      expect_identical(
        sort(as.character(result[[config$column]])),
        sort(as.character(unlist(case$expected)))
      )
    }
    query("json", user = case$user)
    query("arrow", user = case$user)
    query("arrow", options = list(roles = unlist(case$roles)))
    query(
      "arrow",
      options = list(
        roles = config$custom_data_role,
        customData = case$custom_data
      )
    )
  }
})
