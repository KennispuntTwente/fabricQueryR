# Generic shinyOAuth improvements for integration with other R packages

Design proposal, 20 September 2026. No implementation.

**Recommendation:** make shinyOAuth easier to use as the owner of a user's
authorization when another R library performs the actual API or database call.
Expose this through `connection$access_token()`, and let the ordinary
`oauth_module_server()` return a connection through `auth$connection()`.
Both the single-authorization module and the multiple-authorization manager
should provide the same `OAuthConnection` interface from the first release.
Keep the public concepts about OAuth authorizations, access tokens, resources,
and their lifetimes. Service catalogs, query interfaces, and workload permissions
belong in the consuming package.

This refines the shinyOAuth portion of the earlier
[Shiny–Fabric design](shiny-fabric-integration.md). In particular, I would **not
introduce a public Microsoft-specific authorization manager**. The small first
release and the larger optional resource-token feature should be separate work.

All new methods and arguments below are proposals. Current behavior was
checked against shinyOAuth `0.6.1.9000`, revision
`803323c8ced957b43f5638cb11f5ccb97f3317f9`.

**Ergonomics review:** two additions earn their place alongside token access:
`connection$has_scopes()` for optional-feature UI and `auth$reauthorize()` for
an explicit "Sign in again" action. Keep ordinary app code short with
`connection <- shiny::req(auth$connection())`. Specify async return types,
recovery errors and reference lifetimes before adding more convenience methods.
HTTP refresh and automatic selection of a sole managed connection are useful
follow-ups; neither needs another public class or Fabric-specific abstraction.

**1. What belongs where**

| Responsibility | Owner |
| --- | --- |
| OIDC validation, OAuth callbacks, PKCE, session ownership | shinyOAuth; reuse existing behavior |
| Access-token retrieval, refresh coordination, expiration, logout | shinyOAuth |
| Optional management of several resource tokens under one authorization | shinyOAuth, through a generic opt-in model |
| A provider's documented way of selecting resources and expressing scopes | A small provider implementation within shinyOAuth |
| Fabric/SQL/Storage/Kusto audience aliases and delegated scope bundles | fabricQueryR |
| Database connections, queries, inserts, request retries and workload errors | fabricQueryR or another consuming library |
| Which destinations receive exported credentials, and closing authenticated connections | The consuming library/application |
| Business authorization, form validation, result caching and application UI | The application, with optional consuming-package helpers |

Provider-specific protocol code is already a normal part of shinyOAuth. The
design constraint is that it implements a reusable authentication contract:
there should be no Fabric item types, Fabric scopes, fabricQueryR dependency,
or Fabric-specific UI in shinyOAuth.

**2. Existing foundations and the actual gaps**

| Existing foundation | Gap relevant to other packages |
| --- | --- |
| `oauth_module_server()` exposes a reactive `OAuthToken`; `oauth_connection()` can wrap it | Single-authorization apps can already create a connection manually, but the module does not return one directly or give that wrapper a callable refresh controller. |
| `OAuthConnection$request()` resolves owned current credentials | A database driver or SDK cannot use this HTTP transport. There is no public managed-connection access-token accessor. |
| Managed `OAuthConnection$refresh()` coordinates refresh | External consumers need current-token retrieval and expiry handling without separately inspecting, refreshing and rereading private state. |
| `oauth_provider()` already supports `userinfo_required = FALSE` | The Microsoft convenience constructor hardcodes UserInfo retrieval; callers cannot select the existing alternative through that preset. |
| `oauth_client(resource = ...)` supports resource indicators | Resource selection is attached to the client configuration; it is not a cache of independently usable resource tokens. |
| `resource_bases` restricts destinations for connection HTTP calls | An approved destination is not a token acquisition target, nor evidence of an opaque token's audience. |
| `oauth_connections()` retains several independent authorizations | Several tokens derived from one authorization need a different internal relationship to their shared refresh credential. |

These findings come from the
[connection class](../../../shinyOAuth/R/classes__OAuthConnection.R),
[connection helpers](../../../shinyOAuth/R/methods__connection.R),
[single-authorization module](../../../shinyOAuth/R/oauth_module_server.R),
[client configuration](../../../shinyOAuth/R/classes__OAuthClient.R),
[Microsoft preset](../../../shinyOAuth/R/providers.R),
[connection manager](../../../shinyOAuth/R/oauth_connections.R), and
[manager server](../../../shinyOAuth/R/oauth_connections_server.R).

The existing owner checks, stores, refresh coordination and typed errors are
valuable. Extend their contracts rather than introducing a parallel login or
credential-management system.

**3. First change: one connection interface for simple and multi-account apps**

**Get the connection directly from either module.** The single-authorization
module already supports this manual construction today:

```r
auth <- oauth_module_server("auth", client)
connection <- oauth_connection(client, shiny::reactive(auth[["token"]]))
```

The wrapper reads the module's current token, but does not provide coordinated
on-demand refresh. A simple app should not need to supply this wiring or adopt
the multiple-authorization manager just to use an external SDK.

Add `connection()` to the existing single-module return object. Keep its
`reactiveValues` return type, current fields and callable helpers intact.
The proposed ordinary query-callback app looks like this, given a configured
`client` and an external SDK function:

```r
ui <- oauth_ui(
  shiny::fluidPage(
    shiny::actionButton("login", "Sign in"),
    shiny::actionButton("logout", "Sign out"),
    shiny::tableOutput("records")
  ),
  id = "auth",
  client = client
)

server <- function(input, output, session) {
  auth <- oauth_module_server("auth", client, auto_redirect = FALSE)
  shiny::observeEvent(input$login, auth$request_login(), ignoreInit = TRUE)
  shiny::observeEvent(input$logout, auth$logout(), ignoreInit = TRUE)

  output$records <- shiny::renderTable({
    connection <- shiny::req(auth$connection())  # Proposed factory.

    existing_sdk_read_records(
      access_token = connection$access_token(required_scopes = "records.read")
    )
  })
}

app <- shiny::shinyApp(ui, server, uiPattern = ".*")
```

`existing_sdk_read_records()` represents whichever external library the app
uses. The example reuses the existing [UI wrapper](../../../shinyOAuth/R/oauth_ui.R)
and login/logout methods; connection access should not require a new UI module.
It shows the successful read path, with recovery outcomes specified below.
The same connection methods then work in both cases:

| Module | Obtain a connection inside the owning session | Use it |
| --- | --- | --- |
| `auth <- oauth_module_server("auth", client)` | `auth$connection()` — proposed | `connection$access_token()`, `$request()`, `$summary()`, etc. |
| `auth <- oauth_connections_server("auth", manager)` | `auth$connection(id)` — already exists | The same methods, for the selected authorization. |

For the new single-module factory:

- Return `NULL` before authorization and after logout. Read it inside reactive
  code; `shiny::req(auth$connection())` both handles that normal empty state and
  returns the connection. Do not add a separate `require_connection()` helper.
- Return the same reference while the current authorization remains in place,
  including ordinary token refresh and recoverable expiry. Existence does not
  imply that an operation will succeed; the operation checks token readiness.
- Bind each reference to that authorization and the owning Shiny session.
  Logout, authorization replacement or session closure invalidates it. A new
  authorization produces a new reference, even for the same provider account.
  An old reference must not silently start acting for a later login.
- Make the factory depend reactively on authorization changes, not every
  access-token rotation. Readiness/status methods may have finer dependencies.
  A credential refresh alone should not rerun the example's SDK call.

The factory is intentionally a method, matching `auth$connection(id)` on the
manager. The returned connection is an ordinary `OAuthConnection`, not a
reactive function: use `connection$access_token()`, not `connection()$...`.
An app may wrap the factory in `reactive()` if it wants to share the current
reference among several outputs.

**Small manager convenience.** Later allow its existing method to omit the
identifier: `auth$connection()` returns the sole locally owned connection, or
`NULL` when none exists. With several selectable authorizations, raise a
selection-required condition. Count expired/limited connections too; never
silently choose whichever account happens to have a fresh token. Keep explicit
`auth$connection(connection_id)` behavior intact. This makes a one-connection
app easier to migrate to the manager without introducing implicit account
switching into an already captured reference.

**Retrieve the token through a connection method.** Add:

```r
connection$access_token(
  required_scopes = character(),
  min_valid_for = 60,
  force_refresh = FALSE,
  async = FALSE
)
```

Use a method with parentheses, rather than a bare `$access_token` property:
retrieval can refresh credentials and accepts scope/lifetime requirements.
There is no separate exported token-getter function or writable token field.

Return one bearer-token string, suitable for a third-party API client or
database driver. This is an explicit secret-export API: the string must never
be printed, logged, sent to the browser, or treated as an identity claim.
Keeping the return value simple avoids adding another public token class merely
to satisfy consumers that already require a string. Existing summaries expose
nonsecret status and expiry separately.

The operation should:

1. Resolve the connection through its existing session/owner checks.
2. Validate `required_scopes` against configured permissions and retained local
   scope restrictions, in addition to the client's required scopes. This is an
   operation requirement, not a request for additional consent or scope narrowing.
3. Reuse the current token only when lifecycle state permits, its grant covers
   those requirements under the established scope policy, and its known remaining
   lifetime meets `min_valid_for` seconds, allowing for clock skew.
4. Ask the existing lifecycle owner to refresh when freshness requires it or
   `force_refresh = TRUE` bypasses the cache. A known insufficient grant fails
   without attempting to repair it by refresh; additional-target acquisition
   follows the separate rules in section 5. Never force an interactive login.
5. Recheck ownership, authorization identity, expiry and scopes after refresh,
   before delivering the result.
6. Fail clearly if the requirement cannot be met. Never return an expired token
   or initiate an unrelated fallback login.

Keep acquisition scopes separate from `required_scopes`: passing fewer
operation requirements must not narrow the stored grant. Explicit narrowing
continues to use `$refresh(scopes = ...)`.

Bound refresh attempts. A provider may issue a token shorter-lived than
`min_valid_for`; that should produce a useful error, not a refresh loop. Do not
require the returned token string to differ from the previous string: the
guarantee is that forced acquisition occurred and the result was checked.

Example, independent of any particular resource provider:

```r
# Inside the owning Shiny session; `connection` is already authorized.
read_records <- function() {
  token <- connection$access_token(
    required_scopes = "records.read",
    min_valid_for = 60
  )
  existing_sdk_read_records(access_token = token)
}
```

This benefits database drivers, storage SDKs and API wrappers that construct
their own requests. Package-specific callbacks can be built around this one
accessor. shinyOAuth need not adopt another package's callback signature or
its meaning of an `audience` argument.

For an SDK that accepts a token callback, create its client once per
authorization, and retrieve a token when the SDK actually performs an operation:

```r
# Inside server(); `example_sdk_client()` stands for a callback-capable SDK.
sdk <- shiny::reactive({
  connection <- shiny::req(auth$connection())
  example_sdk_client(
    token = function() connection$access_token(required_scopes = "records.read")
  )
})
```

The closure captures the current connection, not today's token string and not
a lookup that silently follows any future login. The factory's reactive lifetime
rebuilds the SDK client on authorization replacement. Document that callbacks
must run in the owning session's reactive context. For SDKs that accept only a
string, acquire it immediately before the operation and follow that SDK's own
rules for rebuilding or updating clients; storing a string does not make it
refreshable. No SDK client or authenticated object belongs in global app state.

**Refresh ownership matters.** Support both module-produced and
manager-produced references in the first release. Give the single module's new
factory an internal binding to its existing token store, authorization lifetime
and refresh controller. Both `$access_token()` and the existing `$refresh()`
must use that binding. Calling the factory must not create a second login
module, independent token cache, or separate manager.

Refactor the single module's expiry/proactive refresh paths into a callable
internal operation, preserving its operation epochs, stale-result checks,
refresh pacing and async handling. Share reusable validation and coordination
with managed connections where possible; retain the distinct session/retention
ownership of each module. This is more work than returning today's wrapper,
but makes the simple API deliver its promised behavior.

Preserve `$refresh(scopes = ...)` semantics for both factory paths, including
retaining accepted scope narrowing on later automatic refresh. Do not introduce
a second public refresh API. Its existing return convention remains `TRUE`
or a promise resolving to `TRUE`, according to the owner's async configuration.

Keep `oauth_connection(client, token_reactive)` working for existing callers.
It remains an advanced wrapper whose token source can change with later logins;
do not infer a refresh controller or authorization boundary from an arbitrary
reactive expression. If the new accessor is called on such a wrapper without
a lifecycle binding, raise a configuration condition directing callers to
`auth$connection()`. This limitation concerns manual wrappers, not the regular
single-authorization module's new factory.

**Keep the rest of the method surface small.**

| Member | Recommendation |
| --- | --- |
| `$access_token(...)` | Add; current bearer credential for external libraries, with managed refresh. |
| `$request(...)` | Keep; authenticated HTTP within declared destinations. Existing behavior does not automatically refresh; preserve that contract in this change. |
| `$refresh(scopes = NULL)` | Keep; make it work on the single module's new references too. Most external consumers can rely on `$access_token()` instead. |
| `$is_usable()` | Keep as a local readiness check, without network calls or implicit refresh. A false result does not mean `$access_token()` cannot recover by refreshing. |
| `$has_scopes(scopes)` | Add as a local permission check for optional-feature UI, independent of token expiry. Use the same scope policy as actual operations. |
| `$summary()` | Keep for nonsecret status and expiry. Extend metadata only where needed; avoid duplicate `$status()` and `$expires_at()` methods initially. |
| `$identity(...)` | Keep for selected validated OIDC identity fields; not all OAuth providers establish identity. |
| `$id` | Keep the read-only identifier. The new single-module reference's ID remains stable across refresh and changes on authorization replacement. |

Do not require `$is_usable()` before `$access_token()` in examples: that would
prevent an expired but refreshable token from recovering. Keep login/logout
actions on the owning module, where their scope is clear, instead of adding a
connection `$logout()` with ambiguous effects on retained authorizations.

**Optional permissions deserve a simple check.** Unlike `$is_usable()`, the
proposed `$has_scopes("records.write")` asks whether the current authorization's
recorded permissions cover the operation. It does not refresh or require a
currently unexpired access token. This lets an app show an optional editing
form without inspecting an `OAuthToken` or disabling the form merely because
the access token needs refreshing.

Return a single logical value. Use the same ordinary-OAuth/SMART scope coverage
and evidence rules as operation checks; return `FALSE` for unrequested scopes,
insufficient evidence, narrowed-away scopes or an unavailable authorization.
Malformed scope arguments remain input errors. An expired token can still
have sufficient recorded permissions under a current, refreshable authorization.
The method is reactive to authorization/permission changes, without tracking
unchanged token rotations. It never proves remote resource access: submit
handlers still call `$access_token(required_scopes = "records.write")`, and the
service checks its own item permissions. An empty scope vector adds no scope
requirement; it does not make an invalid reference valid.

**An explicit recovery action belongs on the module.** Add
`auth$reauthorize()` for the single module, with `auth$reauthorize(connection_id)`
as the matching managed operation. The current `request_login()` deliberately
ignores requests while authenticated, including some retained stale-token
states. Requiring developers to call logout followed by login introduces
revocation and browser-state timing into an ordinary "Sign in again" button.
See [the existing login/logout paths](../../../shinyOAuth/R/oauth_module_server.R).

The proposed operation starts a replacement authorization for the configured
client. Invalidate the old local reference and pending credential operations
before starting the flow; failed/cancelled replacement leaves it invalid.
Do not automatically revoke the upstream grant, which can affect other uses.
Publish a new connection/ID on success. For the manager, replace only the
selected authorization and make the replacement ID discoverable through its
existing connection summaries. Do not deliberately replace other records;
retain existing protections for shared refresh credentials, which may require
invalidating related records. Independent authorizations remain independent.
With no current single-module authorization, use the ordinary sign-in path.
An unknown explicit managed ID remains an error, not a request to pick an account.

Use the established local scope restrictions by default. This action does
not silently add permissions, switch to a different configured client, or
guarantee that the provider will show a password/account chooser. It is an
explicit user action, not something `$access_token()` starts after an error.
Keep existing `request_login()` and `logout()` behavior unchanged.

**Useful later: opt-in freshness for HTTP.** Add an optional
`connection$request(..., refresh = TRUE)` mode after the shared acquisition
operation is in place; retain today's behavior by default for compatibility.
It refreshes if needed before sending the request, using the internal controller
also used by `$access_token()`. Do not implement this by exporting a bearer
string: existing DPoP/mTLS request handling must still work. Do not automatically
replay a failed HTTP write. This avoids an eventual inconsistency where SDK
calls handle token freshness but the built-in HTTP method needs manual refresh.

A generic `$token_provider()` callback factory can wait; an R closure already
handles library-specific signatures. Likewise, avoid `$with_token()`,
`$with_connection()` and query/transaction helpers in shinyOAuth. The consumer
knows how its SDK or database connection must be opened, used and closed.

**Credential-only connections.** The current connection APIs require nonempty
`resource_bases`, although `oauth_client()` itself permits an empty vector.
Allow connections from either module factory used only for token export to
omit HTTP bases. `request()` must still reject every undeclared destination;
empty bases never mean unrestricted HTTP access. This avoids requiring a
database integration to invent a dummy HTTP endpoint merely to participate
in credential management.

**Bearer-only export initially.** Reject DPoP or certificate-bound access
tokens for this convenience API. Supplying their string alone is not equivalent
to performing an authenticated request with the required key/certificate.
Existing shinyOAuth transport continues to support those modes. Supporting
external sender-constrained transports can be a separate, explicit contract.

**Async behavior.** With `async = FALSE`, return a string or raise a condition.
With `async = TRUE`, return a promise resolving to a string or rejecting with
the same acquisition condition, including immediately detected authorization
failures. Argument/configuration validation may still fail synchronously.
Keep that explicit default even when the module uses async background refresh:
changing module configuration must not unexpectedly hand a promise to an SDK
expecting a string. The accessor's argument selects how its own acquisition
runs when no refresh is already underway; module configuration still controls
automatic refresh and the existing explicit `$refresh()` convention.
If an asynchronous refresh already owns the credential, synchronous retrieval
must return a structured pending/unavailable condition rather than block the
event loop waiting for that promise. An async caller should join compatible
in-flight work and recheck its own scope/lifetime requirements on completion.
Different target acquisitions serialize their use of a shared refresh credential.
All internal commits stay with the original owner; workers never mutate a
connection or module reactive values.

Async credential retrieval does not make a synchronous SDK or database call
nonblocking. Include a separate background-task recipe using credentials acquired
in the owning session, bounded worker inputs, and rejection of results belonging
to a replaced authorization. Do not export the connection object to a worker.
Shiny's `ExtendedTask` also requires reactive inputs to be read before invocation,
not inside its task function. [Shiny ExtendedTask documentation](https://shiny.posit.co/r/reference/shiny/latest/extendedtask.html).

**Limit of the guarantee.** shinyOAuth can check ownership at retrieval time.
It cannot retract an exported string, constrain the SDK's destination, cancel
an already submitted request, or close a database connection. Consumers must
retrieve credentials near use and manage their own resources. Passing an
allowed URL to the accessor would not enforce what an external driver later
does with the token, so do not present that as a security boundary.

**4. Second change: expose existing UserInfo configuration consistently**

Keep identity validation and UserInfo retrieval as separate choices. The generic
provider already supports this; the work is primarily convenience-constructor
configuration and documentation.

For example, add the existing argument vocabulary to the Microsoft preset:

```r
provider <- oauth_provider_microsoft(
  tenant = tenant_id,
  userinfo_required = FALSE  # Proposed preset argument.
)
```

Preserve its current default. With retrieval disabled, also disable the
UserInfo-to-ID-token matching requirement, while retaining ID-token validation,
nonce checks and the other OIDC protections. Do not silently substitute decoded,
unverified token claims for verified identity. Apply the same configuration
pattern to other presets where useful; no special `fabric = TRUE` flag or new
identity-profile framework is needed.

The immediate Microsoft motivation is that its UserInfo endpoint requires a
Graph access token, while an application may request a token for a different
API. Microsoft recommends validated ID-token information when it supplies the
needed identity fields. [Microsoft UserInfo documentation](https://learn.microsoft.com/en-us/entra/identity-platform/userinfo).

The broader use case is an OIDC app that needs verified login identity without
another profile HTTP request or additional resource authorization. Identity
continues to be read through `OAuthConnection$identity()` where applicable;
there is no need to invent a new public user object for this integration.

**5. Larger optional change: named token targets within an authorization**

The first two changes make external-library integration substantially easier.
**They do not deliver one sign-in with separate tokens for several APIs.** That
requires additional acquisition and storage behavior, or separate authorizations
using today's manager. Keep that distinction explicit in release promises.

This is a generic OAuth use case: RFC 8707 illustrates one authorization for
calendar and contacts, followed by separate tokens for each resource. The
authorization server decides which target resources are allowed.
[RFC 8707, section 2.2](https://www.rfc-editor.org/rfc/rfc8707.html#section-2.2).

Use a generic optional declaration, tentatively `token_targets` on
`oauth_client()`, and a `target` selector on the accessor. For example:

```r
# Design sketch for an explicitly configured RFC 8707-capable provider.
client <- oauth_client(
  provider,
  client_id = registered_client_id,
  redirect_uri = registered_redirect_uri,
  scopes = c("openid", "calendar", "contacts"),
  token_targets = list(
    calendar = list(
      resource = "https://calendar.example/",
      scopes = "calendar"
    ),
    contacts = list(
      resource = "https://contacts.example/",
      scopes = "contacts"
    )
  ),
  default_token_target = "calendar",
  resource_bases = c(calendar_api = "https://calendar.example/v1")
)

# After authorization, inside the owning session:
token <- connection$access_token(target = "contacts")
```

`token_targets` and `default_token_target` are proposed arguments. This sketch
describes acquisition selection; provider configuration must explicitly enable
and validate the supported acquisition behavior. It is not a claim that any
OAuth server accepts these requests or necessarily issues a refresh token.
Keep this selector on the same connection method for both module APIs.
With one declared target, allow it to be the default without another setting.
With several targets, require `default_token_target` at client configuration
time. It selects both the initial code-redemption target and the default for
later accessors; an accessor's `target` argument overrides only that call.
Selection must be settled before sign-in, even if every later call names its
target. Never choose the first list element implicitly.
Later `$has_scopes(..., target = ...)` must inspect that target's recorded
permissions, not the union of unrelated API scopes. A `FALSE` result due to
missing evidence is not proof that the provider would deny an initial acquisition.
If target inspection is useful, add `$targets()` at this later stage to expose
declared target names and nonsecret acquisition status; it is not needed for
the initial single-token interface.

Keep three concepts distinct:

| Concept | Meaning |
| --- | --- |
| Token target | A locally named, declared acquisition request: intended resource and scopes. |
| OAuth resource indicator | A protocol value sent only when the configured provider supports that mechanism. |
| `resource_bases` | Existing allowed destinations for HTTP requests made by shinyOAuth. |

A target name is an application selector, not a magic service name, URL or
proof of permission. Do not infer a resource from an arbitrary URL or split
scope strings on slashes in generic code. An OAuth resource identifier need
not equal a request origin. Validate declarations before starting sign-in.

Apply acquisition and readiness checks to the selected target. First validate
the operation against that target's configuration and retained scope limits;
then acquire if necessary and check the resulting target token's permissions.
A missing cache entry is not itself a missing-consent decision, and a calendar
token cannot establish contacts permissions. Provider policy and the server's
response determine whether acquisition under the existing authorization succeeds.
Keep authorization-wide identity/lifecycle requirements separate from API scopes
required on a particular target. Failure of one optional target must not mark
every other target unusable unless the shared authorization itself has failed.

For compatibility, unconfigured clients retain their current single-token
behavior. Existing `request(resource_id, ...)` calls retain their meaning.
In a later HTTP integration, add explicit target selection and validate its
association with approved resource bases; never silently pick a token because
its target name happens to match a destination alias. The first external-token
release does not require changing `request()` at all.

**Separate authorization from individual token acquisition.** Internally track:

- The authorization transaction, local owner and permitted configuration.
- Validated identity, when the flow actually establishes OIDC identity.
- The refresh credential and its current lifecycle state.
- The explicitly selected primary target for code redemption.
- Access-token entries per target, requested/granted scopes, expiry and token
  binding, plus evidence about how those permissions were established.

Do not enlarge `OAuthToken` into an application-wide token store. It can remain
the representation of one token response. Put the relationship among responses
and their shared refresh credential in an internal authorization record usable
by either lifecycle owner. Keeping fields in that record does not require
exporting a new public authorization class in the first version.

**Different providers can implement this contract differently.**

| Provider behavior | Implementation boundary |
| --- | --- |
| Ordinary single-token OAuth | Existing behavior, with no target switching. |
| Explicit RFC 8707 support | Send the appropriate declared `resource` and `scope` at the relevant protocol stages. |
| Microsoft Entra | Translate declared targets using its documented scope/resource conventions in its existing provider layer. |

Microsoft refresh credentials can acquire tokens for additional resources where
permission exists. Its code flow distinguishes consent scope selection from
resource-specific code redemption. That is one implementation of the generic
feature, not a rule to apply to other providers.
[Microsoft refresh tokens](https://learn.microsoft.com/en-us/entra/identity-platform/refresh-tokens),
[Microsoft authorization code flow](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-auth-code-flow).

Keep the established issuer/account context fixed for the initial feature;
switching tenant or account requires a new authorization, not target selection.

Keep provider behavior explicit and fail for unsupported acquisition modes.
Do not infer support from the presence of an OIDC discovery document. Initially
use a small internal strategy interface for reviewed implementations, rather
than publishing an arbitrary token-exchange callback/plugin framework. Broaden
that extension API only when another concrete integration needs it.

**6. Preserve scope and refresh semantics**

Today's `refresh(scopes = ...)` deliberately narrows a connection's accepted
permissions. Keep that behavior. Selecting a different previously authorized
target is a separate operation; it must not silently restore permissions that
the user/application narrowed on an existing target.

Refresh must not become a way to add unconsented permissions. RFC 6749 requires
requested refresh scopes to stay within the original authorization. A target
declaration is a local limit and request specification, not evidence that the
server granted it. [RFC 6749, section 6](https://www.rfc-editor.org/rfc/rfc6749.html#section-6).

The existing code appropriately rejects `scope` inside `extra_token_params`.
Retain that restriction: target selection needs an explicit model of the
authorization and token request, not an escape hatch that bypasses it.
See [OAuth parameter validation](../../../shinyOAuth/R/utils__oauth_params.R)
and [refresh scope validation](../../../shinyOAuth/R/utils__refresh_scopes.R).

Scope comparison remains literal for ordinary OAuth, with existing SMART
semantics preserved. Provider-specific aliases or `.default` interpretation
belong in a reviewed provider policy. Do not strip prefixes globally, assume
every requested scope was independently granted, or treat the latest access
token's scopes as the complete refresh authorization.

Use one lifecycle owner for resource tokens known to come from the same local
authorization. Serialize refresh-credential use and commit rotated credentials
atomically. Include authorization identity, target policy/configuration and
scope restrictions in cache isolation; do not key solely by resource name.

Do not merge independent connections merely because the provider returned the
same refresh-token bytes. Today's manager treats such aliases conservatively;
that is different from several child token entries intentionally managed under
one authorization. Preserve the existing behavior for independent connections.
Relevant code:
[credential lifecycle](../../../shinyOAuth/R/connection_credential_lifecycle.R)
and [connection store](../../../shinyOAuth/R/connection_store.R).

**7. Small supporting contracts, rather than a new application framework**

**Authorization lifetime.** Reuse existing managed connection IDs and owner
generations wherever sufficient. The single module's new factory needs an
explicit authorization generation as well: its current reactive token source
alone cannot distinguish ordinary refresh from replacement. Prefer to reflect
this through the connection's existing `$id` and lifetime contract; expose extra
nonsecret metadata only if that is insufficient. A store revision is unsuitable
because normal refresh changes it. Bind async completions to the captured
authorization, and do not export mutable store state. These are local
authorization identifiers, not claims about a user's identity; they also work
with ordinary OAuth without OIDC.

**Structured outcomes.** Extend the current condition/status system with stable
reasons where needed: unavailable authorization, insufficient scope, refresh
pending/unavailable, unknown or unsupported target, and interaction required.
Reuse existing classifications where they already suffice. Keep provider
diagnostics separate from displayable messages. A consuming package can decide
whether to show a login button or stop an operation; a token getter should not
unexpectedly redirect the browser or replay a business request.

Make this concrete: give credential-acquisition failures a catchable subclass
of the existing token error and document stable `condition$context$reason`
values using the existing [condition constructors](../../../shinyOAuth/R/errors__constructors.R).
An application should not need to parse an error message:

| Outcome | App response |
| --- | --- |
| No authorization: factory returns `NULL` | Show sign-in UI; `req()` clears dependent output. |
| `refresh_pending` | Show waiting/retry state, or use the promise-returning accessor to await completion. |
| `interaction_required` | Offer the module's explicit reauthorization action. |
| `insufficient_scope` | Explain that the operation is unavailable; repeated refresh is not a remedy. |
| Stale/ended reference or unavailable owner | Discard the dependent client and obtain a current connection if one exists. |
| Invalid target/scope configuration | Surface a developer configuration error; do not present it as a login failure. |

Do not return `NULL`, an empty token or a status list from `$access_token()` on
failure. Keep its successful return type predictable. In synchronous mode,
pending does not promise that a failed reactive will automatically retry: the
app must await asynchronously or offer an explicit retry action. Do not turn
all acquisition errors into silent Shiny cancellations inside the package.

**Reactivity.** Resolve current credentials at use time, but let applications
depend on authorization/readiness changes without depending on every token
rotation. Successful silent refresh should not by itself rerun expensive
queries or reset owner inactivity. Preserve explicit user-activity tracking.
The manager currently treats explicit `$refresh()` as activity; refresh needed
by `$access_token()` should use the internal operation without that side effect.
For the new accessor, isolate reads of changing token values while retaining
reactive dependencies on authorization replacement/invalidation. Keep explicit
status reads reactive. Test these dependencies rather than wrapping the entire
operation in `isolate()`, which would also hide logout and replacement.

These contracts support any session-bound third-party client. They do not
require adding query caching, DBI pools, background-job orchestration or
distributed credential storage to shinyOAuth.

**Examples must distinguish reads, writes and cached results.** The main
example is a read that can rerun. Put inserts/updates in an explicit input-event
handler, acquire the credential inside it, and do not retry the business action
after login or refresh without a new user action. Shiny event handlers isolate
their bodies from ordinary reactive invalidation.
[Shiny event-handler documentation](https://shiny.posit.co/r/reference/shiny/latest/observeevent.html).

Event-bound results and app-owned caches need a separate authorization check
when rendered: a query bound only to a "Load" button will not necessarily
invalidate on logout. Store the connection ID with its result and compare it
with the current factory's reference before display. Keep the comparison
reactive so logout/replacement clears the display. This is also how a consumer
can discard a late background result. Avoid `req(..., cancelOutput = TRUE)`
for this check, because it deliberately keeps the previous output visible.
[Shiny req documentation](https://shiny.posit.co/r/reference/shiny/latest/req).

**8. Recommended delivery sequence**

| Step | Deliverable | Why it earns its place |
| --- | --- | --- |
| 1 | `connection$access_token()` and single-module `auth$connection()`, with shared ownership/lifetime contracts, typed recovery outcomes and refresh support in both module paths | Makes external-library integration equally straightforward for one authorization or several. |
| 1a | `$has_scopes()` and module-level `$reauthorize()`, with complete app and SDK-callback examples | Supports optional features and recovery without private token access or manual logout/login sequencing. Aim to include these in the first release. |
| 2 | Preset parity for existing optional UserInfo retrieval | Small configuration improvement useful to OIDC apps independently of any downstream package. |
| 3 | Validate the target-acquisition model against Microsoft and an RFC 8707 test provider | Establish that the public concepts are portable before changing authorization storage. |
| 4 | Opt-in target declarations, per-authorization token entries and provider acquisition implementations | Delivers one-authorization/multiple-token use cases without affecting ordinary clients. |
| 5 | Document target selection and multi-resource lifetime handling | Completes the larger feature's examples; single- and multiple-authorization examples ship with step 1. |

Steps 1 and 2 are independently releasable. Steps 3 and 4 are required for the
seamless multi-resource goal; they should not be hidden inside a small adapter
patch. Use a real non-Microsoft deployment before claiming broad provider
interoperability; a standards-based test server establishes a contract, not
universal service compatibility.

The sole-connection manager lookup and opt-in HTTP refresh are small follow-ups
after the core contract is proven. Do not delay the external-token interface
to build a general action runner, task framework or new UI component library.

The first release need not add multiple-worker storage, automatic incremental
consent, OBO/token exchange, generic provider plugins, sender-constrained token
export, or new Shiny UI components. Those are separate features with their own
users and validation requirements.

**9. Acceptance criteria**

| Area | Evidence required |
| --- | --- |
| Module parity | A single-module app obtains `auth$connection()` without manually supplying a token reactive; the same external SDK example works with `auth$connection(id)` from the manager. |
| External access | A non-Fabric SDK/driver consumes a current token; no refresh token or private connection fields are exposed. |
| Ownership | Cross-session and ended references fail; logout during async refresh cannot deliver newly usable credentials. |
| Reference lifetime | The single factory returns `NULL` before login/after logout; its reference and ID survive refresh, change on new authorization, and old references remain invalid after re-login. |
| Lifetimes | Expiry buffer, missing refresh support, forced refresh and too-short new tokens behave without loops. |
| Single-module refresh | Manual, proactive and accessor-triggered refresh coordinate; accepted scope narrowing persists; late completions cannot restore an ended/replaced authorization. |
| Optional scopes | `$has_scopes()` follows existing scope/evidence policy, does not refresh, reflects narrowing, and does not confuse token expiry with a missing permission. |
| Reauthorization | A retained stale login can start replacement explicitly; old references/late completions fail, cancellation does not revive them, and unrelated managed connections stay usable. |
| Async contract | Cached async retrieval returns a promise; immediate auth failures reject it; sync retrieval never returns a promise; concurrent compatible acquisitions share work, and outcomes are machine-readable. |
| Reactivity | Token rotation alone does not repeat the example's SDK request; logout/replacement invalidates consumers, and explicit status reads still update. |
| App examples | Callback-based SDKs see fresh credentials; cached/event-bound results clear on logout/replacement; writes require an input event and are not replayed by auth recovery. |
| Token binding | DPoP/mTLS credentials are rejected by the bearer export path; existing bound-token transport still works. |
| Identity | Disabling UserInfo leaves OIDC validation intact and makes no UserInfo request on login or refresh. |
| Resources | The initial target is fixed before sign-in; two declared targets retain distinct usable tokens; an undeclared target fails before exchange; optional-target failure does not disable unrelated usable targets. |
| Permissions | Operation checks do not narrow acquisition scopes; target switching does not widen narrowed permissions; omitted/partial scope evidence follows the provider rules. |
| Refresh coordination | Concurrent target acquisitions, rotation, ambiguous outcomes, logout and stale completions use one coherent lifecycle. |
| Compatibility | Existing module return fields/helpers, manually constructed wrappers, single-token, independent-connection, SMART and HTTP destination-policy behavior remains unchanged. |
| Later conveniences | Sole-connection lookup rejects ambiguity; opt-in HTTP refresh sends the application request once and preserves bound-token transport. |
| Portability | Public docs/examples make sense using generic calendar/contacts APIs; Fabric scope/endpoint details exist only in the consumer. |

This document is based on source and protocol review. No code changed and no
new behavior was executed. The focused tests reported in the broader design
cover existing building blocks only; they do not validate these proposed APIs.
