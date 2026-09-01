# fabricQueryR development instructions

## Local Fabric credentials

Local development may use these environment variables:

- `FABRICQUERYR_TENANT_ID`
- `FABRICQUERYR_CLIENT_ID`
- `FABRICQUERYR_CLIENT_SECRET`

Treat their values as secrets. Never print them, include them in command
output, write them to repository files, commit them, or place them in
test snapshots. It is safe to refer to the variable names in code and
documentation.

When all three variables are present, the Python sandbox commands use an
Azure client-secret credential for local `deploy`, `seed`, and
`discover` operations:

``` text
uv --directory tools/fabric-sandbox sync --locked
uv --directory tools/fabric-sandbox run fabric-sandbox doctor
uv --directory tools/fabric-sandbox run fabric-sandbox deploy
uv --directory tools/fabric-sandbox run fabric-sandbox seed
uv --directory tools/fabric-sandbox run fabric-sandbox discover
```

The target workspace/item environment variables still need to identify
the intended development sandbox. Do not rebuild, remove, or clean up
Fabric resources unless the task explicitly authorizes that state
change.

For the persistent local R integration suite, run from the repository
root:

``` r

source("tools/fabric-sandbox/local-integration.R")
run_fabric_integration_tests()
```

The runner first reuses matching cached AzureAuth tokens. If a matching
token is unavailable and the three `FABRICQUERYR_*` variables are
configured, it uses AzureAuth’s client-credentials flow. Its final
fallback is AzureAuth’s normal interactive sign-in. The application or
signed-in user must already have access to the target Fabric workspace.

Do not use the slow integration workflow as the first execution of
changed behavior. Run focused offline code/tests during implementation,
then use the persistent runner for every change that crosses a live
Fabric boundary. If a source-controlled item changed, deploy only its
exact repository `Name.Type` before the filtered test:

``` r

run_fabric_integration_tests(
  filter = "integration-fabric-jobs",
  deploy_items = "TestPipeline.DataPipeline"
)
```

Set `seed_fixtures = TRUE` only when the seed notebook or fixture inputs
changed. For `integration-fabric-jobs`, the runner automatically uses a
jobs-only seed and manifest, so unrelated SQL, Kusto, Power BI, or
mirroring readiness cannot block the early job test. The complete
decision guide is in
`.codex/skills/fabric-development-workflow/SKILL.md`.

## Roxygen2 documentation

Write public documentation for a typical R user first. Assume the reader
knows R, but may not know Fabric’s REST APIs, Azure terminology, storage
protocols, token audiences, or internal response formats.

- Start each topic with the user task and the result, in plain language.
- Put the simplest and most common workflow before advanced
  alternatives.
- Prefer Fabric portal terms that users can see, and briefly explain an
  unavoidable specialist term the first time it appears.
- Keep parameter descriptions practical: say what to pass, when it is
  needed, and what the default does. Avoid repeating implementation
  details there.
- Move permissions, service limits, protocol behavior, exact type
  mappings, retry rules, and unusual authentication flows into clearly
  named sections after the basic usage guidance.
- Keep technical detail when it affects correctness, security, data
  loss, precision, performance, or troubleshooting, but do not make it
  the opening explanation.
- Use examples based on discovered Fabric records when that is the
  easiest workflow; show raw IDs, endpoints, or connection strings as
  alternatives.
- Before adding detail, ask whether it helps most users choose, call, or
  understand the function. If it serves maintainers rather than users,
  prefer an internal comment or developer documentation.

When changing a public function, review its whole help topic for
readability; do not append new implementation notes to an already dense
introduction.

## Vignettes

Write vignettes for people who are still learning both fabricQueryR and
Microsoft Fabric. Assume readers know basic R syntax, but may be
unfamiliar with Fabric, cloud storage, SQL/KQL/DAX/GraphQL, and
data-engineering concepts.

- Open every vignette with the user goal and a gentle explanation of the
  basic Fabric concepts needed for that goal. Define specialist terms on
  first use.
- Show the smallest common workflow before permissions matrices, API
  behavior, scaling, performance tuning, failure recovery, or
  implementation detail.
- Prefer discovered workspace and item records, small data frames, and
  results that readers can inspect. Avoid requiring copied IDs and
  endpoints in the first example when discovery can supply them.
- Explain why a reader would choose the featured workflow and, when
  several package features overlap, compare them directly and link to
  the relevant overview or deep-dive vignette.
- Order material from basic and broadly useful to advanced, niche, or
  workload-specific. Advanced detail is welcome after the beginner path
  is complete, especially when it affects correctness, security, data
  loss, precision, performance, or troubleshooting.
- Keep examples approachable for less-experienced R users. Avoid
  introducing an extra package or advanced R idiom when base R or a
  direct fabricQueryR call is equally clear.
- Maintain vignette coverage for every core feature group, with
  particular attention to the alternative workflows for bringing Fabric
  data into R and sending R or Arrow data into Fabric.

When adding or substantially changing a core feature, review the
vignette set as a learning path rather than appending an isolated
advanced article. Update the `articles` order in `_pkgdown.yml` so
introductory and comparative guides appear before deep dives.

## User-facing messages, conditions, and progress

Route every user-facing informational message and printed package
summary through `cli`. Use `inform()` for optional lifecycle messages
controlled by a `verbose` argument, direct semantic `cli` functions for
other output, and `.fabric_print()` for concise package-object
summaries. Do not use
[`message()`](https://rdrr.io/r/base/message.html),
[`cat()`](https://rdrr.io/r/base/cat.html), or
[`writeLines()`](https://rdrr.io/r/base/writeLines.html) for user-facing
output.

Raise new errors and warnings through `.fabric_abort()` and
`.fabric_warn()`. These package helpers apply `cli` dynamic formatting
and then call
[`rlang::abort()`](https://rlang.r-lib.org/reference/abort.html) or
[`rlang::warn()`](https://rlang.r-lib.org/reference/abort.html),
preserving caller context, parent conditions, classes, and metadata. Set
`.format = TRUE` only for trusted message templates written in package
code. Keep service responses and other runtime-supplied messages literal
so they are never evaluated as `cli` glue expressions. Never use
[`stop()`](https://rdrr.io/r/base/stop.html),
[`warning()`](https://rdrr.io/r/base/warning.html),
[`cli::cli_abort()`](https://cli.r-lib.org/reference/cli_abort.html), or
[`cli::cli_warn()`](https://cli.r-lib.org/reference/cli_abort.html).
Re-signal an existing condition with
[`rlang::cnd_signal()`](https://rlang.r-lib.org/reference/cnd_signal.html)
only when preserving that exact condition is required.

Write messages consistently:

- State the outcome or problem first in sentence case, without trailing
  punctuation
- Use `cli` markup such as `{.arg name}`, `{.fn function}`,
  `{.path path}`, `{.field field}`, and `{.val value}` instead of manual
  quotes or backticks
- Use a short main line plus `x`, `!`, or `i` bullets when context or a
  recovery action is useful; avoid repeating the same fact in multiple
  bullets
- Use `cli` pluralization instead of manual singular/plural branches
- Use the same resource names throughout a workflow, including
  `Fabric job`, `Fabric operation`, `Power BI refresh`,
  `Kusto ingestion`, and `KQL export`

Long-running polling must use `.fabric_poll_progress()`, update it with
`.fabric_poll_progress_update()` after each received service state, and
finish it with `.fabric_poll_progress_done()` before returning a
terminal result. Let `cli` delay progress display so fast operations
remain quiet, and respect an existing `verbose = FALSE` setting. For
work with a known total, use an appropriate `cli_progress_bar()` type
and update it only after confirmed work; use download-style byte
progress for uploads and downloads. Rely on `cli`’s automatic cleanup on
errors, but close successful progress explicitly.

## R code organization and maintainability

Apply these rules to every file in `R/`. Treat them as part of the
definition of done for new code and for refactors that touch an existing
function.

### Order code from public workflows to implementation details

- Put exported functions and the file’s main entry points near the top,
  after any constants or package-level documentation they need.
- When a file has several public functions, keep the normal user
  workflow first. Put closely related S3 methods or R6 classes after
  their public constructors.
- Place internal orchestration helpers after the public API, then
  increasingly smaller, more specialized parsing, conversion,
  validation, and formatting helpers toward the bottom.
- Review the entire file after adding or moving a function; do not leave
  a main entry point below its low-level helpers.

### Make long functions readable as numbered steps

- Split long functions, especially public functions and complex
  orchestration helpers, into numbered sections that describe the
  workflow in plain language.
- Format a top-level section as `# 1 Section title` followed by enough
  `-` characters to make the line exactly 100 columns. Use
  `## 1.1 Section title` for a subsection and `### 1.1.1 Section title`
  for deeper nesting, with the same 100-column rule.
- Leave exactly two empty lines between the end of one section and the
  next section heading.
- Every section heading must be followed by one empty line, a short
  beginner-friendly explanation, and another empty line before its first
  R statement. Explain both what the section does and why that step is
  needed.
- In longer or more complicated sections, add concise inline comments
  before non-obvious branches, safety checks, protocol workarounds, or
  conversions. Explain what happens and why; avoid restating individual
  lines of code.
- Do not end R comment lines with a full stop. This applies to section
  explanations, inline comments, helper descriptions, and roxygen2
  lines. Question marks, code punctuation, URLs, and punctuation
  required by an R expression are exceptions.

### Use whitespace and comments to show code structure

- Group adjacent statements that perform one small piece of work, and
  place one empty line before the code moves to a different validation,
  transformation, request phase, branch purpose, or return decision.
- Do not leave long runs of unrelated statements visually glued
  together. Use whitespace at complete expression boundaries; never
  split a single call or condition merely to add space.
- Add short inline guide comments to long snippets that do not warrant
  numbered subsections. Comments should introduce a meaningful group of
  statements or explain a non-obvious decision, not narrate each line.
- Keep related setup assignments together. Excessive one-line groups are
  as distracting as too little whitespace, so use blank lines to show
  real changes in purpose rather than after every statement.

### Keep helpers useful and documented

- Do not create a thin internal wrapper that merely renames one call,
  filters a list once, or forwards arguments and is used in only a few
  places. Put that expression at the call site unless the helper
  enforces a repeated safety rule, provides a deliberate test seam, or
  removes substantial duplicated logic.
- When touching a helper, inspect its call sites. Inline and remove
  helpers that no longer earn their indirection, and remove dead helpers
  rather than leaving unused compatibility code.
- Look for duplicated validation, request construction, parsing, and
  conversion logic. Reuse an existing substantive helper when it
  improves clarity; do not introduce abstraction solely to reduce a line
  count.
- Remove obsolete branches, repeated work, unnecessary temporary
  objects, and code made unreachable by the final design. Verify
  removals with repository search and tests.
- Give every internal function a short beginner-friendly description
  immediately above it. State what its inputs mean, what it returns (or
  that it raises), why it exists, and which workflow uses it. Prefer
  internal roxygen2 with `@param`, `@return`, `@keywords internal`, and
  `@noRd` for substantive helpers; a concise ordinary comment is
  acceptable for a very small local helper or explicit test seam.
- Keep internal documentation practical and brief. Avoid unexplained
  protocol jargon and implementation trivia that does not help a new
  maintainer follow the code.

## NEWS.md

Treat the current development section as release notes for users, not as
a development log. Its comparison point is the latest git-tagged
release.

- Before editing NEWS, inspect the net change from the latest release
  tag to `HEAD` (for example with `git diff <tag>..HEAD` and
  `git log <tag>..HEAD`).
- Include the most important user-visible additions, behavior changes,
  deprecations, compatibility breaks, and fixes to functionality that
  existed in the tagged release.
- Describe the final behavior of a feature once. Fold later fixes and
  refinements to a feature introduced during the same development cycle
  into its original entry; do not give those intermediate changes
  separate bullets.
- Omit internal refactors, test and CI work, documentation-only edits,
  routine validation details, and implementation mechanics unless they
  have a material consequence for package users.
- Write concise, plain-language bullets organized by user task. Mention
  function or argument names when they help users find or adapt to a
  change, but avoid protocol details and exhaustive lists of edge cases.
- Re-audit the whole development section when updating it. Remove
  entries that became obsolete, were superseded, or describe differences
  only between untagged development commits.
