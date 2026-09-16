Publish `user-data-functions.py` as a User Data Functions item in the marked
integration workspace. Publishing requires the item's delegated owner; the CI
service principal cannot maintain its definition. Enable Public access for
`scalar`, `structured`, and `error`, and copy each Public URL from Run only mode.

Set `FABRIC_TEST_FUNCTION_SCALAR_URL`, `FABRIC_TEST_FUNCTION_STRUCTURED_URL`, and
`FABRIC_TEST_FUNCTION_ERROR_URL` in the local session or as GitHub environment
secrets. Grant the integration identity Execute access. The functions echo
inputs or deliberately fail and do not modify data. The `error` fixture must
include both the handled -1 case and the unhandled -2 case.

Run `run_fabric_integration_tests(filter = "integration-fabric-functions")`.
Application tests use AzureAuth's client-credentials settings and the package's
default audience, both directly and through a reused credential. The delegated
test runs only when the runner has a delegated authentication context; its app
must have the Power BI `UserDataFunction.Execute.All` delegated scope. No test
overrides `audience`, so a regression in default audience selection is visible.

With no URLs configured, the lane reports NOT EXERCISED. Once a fixture is
configured, missing companion URLs fail required integration runs. A successful
application lane does not imply that delegated execution was exercised.

For the designated Functions lane, set `FABRIC_TEST_REQUIRED_FEATURES=functions`.
It fails even when all three URLs are missing, so an unconfigured run cannot pass
as Functions validation. The unhandled Python exception test accepts the
documented HTTP 409 or the observed runtime HTTP 500 envelope, and checks the
failed status, errors, and invocation ID. Internal exception messages can be
redacted by the service.
