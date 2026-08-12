# fabricQueryR development instructions

## Local Fabric credentials

Local development may use these environment variables:

- `FABRICQUERYR_TENANT_ID`
- `FABRICQUERYR_CLIENT_ID`
- `FABRICQUERYR_CLIENT_SECRET`

Treat their values as secrets. Never print them, include them in command
output, write them to repository files, commit them, or place them in test
snapshots. It is safe to refer to the variable names in code and documentation.

When all three variables are present, the Python sandbox commands use an Azure
client-secret credential for local `deploy`, `seed`, and `discover` operations:

```text
uv --directory tools/fabric-sandbox sync --locked
uv --directory tools/fabric-sandbox run fabric-sandbox doctor
uv --directory tools/fabric-sandbox run fabric-sandbox deploy
uv --directory tools/fabric-sandbox run fabric-sandbox seed
uv --directory tools/fabric-sandbox run fabric-sandbox discover
```

The target workspace/item environment variables still need to identify the
intended development sandbox. Do not rebuild, remove, or clean up Fabric
resources unless the task explicitly authorizes that state change.

For the persistent local R integration suite, run from the repository root:

```r
source("tools/fabric-sandbox/local-integration.R")
run_fabric_integration_tests()
```

The runner first reuses matching cached AzureAuth tokens. If a matching token
is unavailable and the three `FABRICQUERYR_*` variables are configured, it
uses AzureAuth's client-credentials flow. Its final fallback is AzureAuth's
normal interactive sign-in. The application or signed-in user must already
have access to the target Fabric workspace.

## Roxygen2 documentation

Write public documentation for a typical R user first. Assume the reader knows
R, but may not know Fabric's REST APIs, Azure terminology, storage protocols,
token audiences, or internal response formats.

- Start each topic with the user task and the result, in plain language.
- Put the simplest and most common workflow before advanced alternatives.
- Prefer Fabric portal terms that users can see, and briefly explain an
  unavoidable specialist term the first time it appears.
- Keep parameter descriptions practical: say what to pass, when it is needed,
  and what the default does. Avoid repeating implementation details there.
- Move permissions, service limits, protocol behavior, exact type mappings,
  retry rules, and unusual authentication flows into clearly named sections
  after the basic usage guidance.
- Keep technical detail when it affects correctness, security, data loss,
  precision, performance, or troubleshooting, but do not make it the opening
  explanation.
- Use examples based on discovered Fabric records when that is the easiest
  workflow; show raw IDs, endpoints, or connection strings as alternatives.
- Before adding detail, ask whether it helps most users choose, call, or
  understand the function. If it serves maintainers rather than users, prefer
  an internal comment or developer documentation.

When changing a public function, review its whole help topic for readability;
do not append new implementation notes to an already dense introduction.

## R code organization and maintainability

Apply these rules to every file in `R/`. Treat them as part of the definition of
done for new code and for refactors that touch an existing function.

### Order code from public workflows to implementation details

- Put exported functions and the file's main entry points near the top, after
  any constants or package-level documentation they need.
- When a file has several public functions, keep the normal user workflow first.
  Put closely related S3 methods or R6 classes after their public constructors.
- Place internal orchestration helpers after the public API, then increasingly
  smaller, more specialized parsing, conversion, validation, and formatting
  helpers toward the bottom.
- Review the entire file after adding or moving a function; do not leave a main
  entry point below its low-level helpers.

### Make long functions readable as numbered steps

- Split long functions, especially public functions and complex orchestration
  helpers, into numbered sections that describe the workflow in plain language.
- Format a top-level section as `# 1 Section title ` followed by enough `-`
  characters to make the line exactly 100 columns. Use `## 1.1 Section title`
  for a subsection and `### 1.1.1 Section title` for deeper nesting, with the
  same 100-column rule.
- Leave exactly two empty lines between the end of one section and the next
  section heading.
- Add a short beginner-friendly comment below a heading when the purpose or
  reason for that step is not already obvious from the heading.
- In longer or more complicated sections, add concise inline comments before
  non-obvious branches, safety checks, protocol workarounds, or conversions.
  Explain what happens and why; avoid restating individual lines of code.

### Keep helpers useful and documented

- Do not create a thin internal wrapper that merely renames one call, filters a
  list once, or forwards arguments and is used in only a few places. Put that
  expression at the call site unless the helper enforces a repeated safety rule,
  provides a deliberate test seam, or removes substantial duplicated logic.
- When touching a helper, inspect its call sites. Inline and remove helpers that
  no longer earn their indirection, and remove dead helpers rather than leaving
  unused compatibility code.
- Look for duplicated validation, request construction, parsing, and conversion
  logic. Reuse an existing substantive helper when it improves clarity; do not
  introduce abstraction solely to reduce a line count.
- Remove obsolete branches, repeated work, unnecessary temporary objects, and
  code made unreachable by the final design. Verify removals with repository
  search and tests.
- Give every internal function a short beginner-friendly description immediately
  above it. State what its inputs mean, what it returns (or that it raises), why
  it exists, and which workflow uses it. Prefer internal roxygen2 with `@param`,
  `@return`, `@keywords internal`, and `@noRd` for substantive helpers; a concise
  ordinary comment is acceptable for a very small local helper or explicit test
  seam.
- Keep internal documentation practical and brief. Avoid unexplained protocol
  jargon and implementation trivia that does not help a new maintainer follow
  the code.

## NEWS.md

Treat the current development section as release notes for users, not as a
development log. Its comparison point is the latest git-tagged release.

- Before editing NEWS, inspect the net change from the latest release tag to
  `HEAD` (for example with `git diff <tag>..HEAD` and `git log <tag>..HEAD`).
- Include the most important user-visible additions, behavior changes,
  deprecations, compatibility breaks, and fixes to functionality that existed
  in the tagged release.
- Describe the final behavior of a feature once. Fold later fixes and
  refinements to a feature introduced during the same development cycle into
  its original entry; do not give those intermediate changes separate bullets.
- Omit internal refactors, test and CI work, documentation-only edits, routine
  validation details, and implementation mechanics unless they have a material
  consequence for package users.
- Write concise, plain-language bullets organized by user task. Mention
  function or argument names when they help users find or adapt to a change,
  but avoid protocol details and exhaustive lists of edge cases.
- Re-audit the whole development section when updating it. Remove entries that
  became obsolete, were superseded, or describe differences only between
  untagged development commits.
