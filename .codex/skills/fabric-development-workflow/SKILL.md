---
name: fabric-development-workflow
description: Use when changing or reviewing the fabricQueryR package, a pull request, R code, live Microsoft Fabric behavior, sandbox tooling, infra/fabric item definitions, or Fabric integration tests. Also use when assessing whether package changes have enough execution evidence, or when an agent proposes waiting for Fabric CI to learn whether executable changes work. Skip prose-only edits that cannot affect executable examples.
---

# Fabric development workflow

Get useful execution evidence before handing work to CI. CI is the final
cross-platform and clean-provisioning check; it must not be the first execution
of changed behavior when the local package and persistent Fabric sandbox can
exercise it sooner.

## Protect the development sandbox

- Treat `FABRICQUERYR_TENANT_ID`, `FABRICQUERYR_CLIENT_ID`, and
  `FABRICQUERYR_CLIENT_SECRET` as secrets. Check only whether they exist; never
  print, persist, snapshot, or interpolate their values into commands.
- Let the local runner manage child-process authentication. Do not copy or
  replay bearer tokens manually; the runner supplies refreshable client
  credentials for long sandbox operations when the configured secret is used.
- Use the marked `fabricqueryr-dev-dhrkoning` persistent workspace for live
  development. The R runner verifies its ownership marker before mutation.
- Select only the item definitions needed by the change. Reseed only when
  fixture inputs or the seed notebook changed because seeding is relatively
  expensive and rewrites shared development data.
- Prefer a supported focused fixture scope over a full seed. The jobs filter
  uses an independent revision marker and does not wait for unrelated SQL,
  Kusto, Power BI, or mirroring fixtures.
- Do not rebuild, tear down, or provision a workspace merely to get a clean
  test. Those operations are slow and destructive. Use them only when the
  persistent workspace is absent or structurally incompatible and the task
  authorizes that lifecycle change.

## Choose the shortest truthful feedback loop

Inspect the changed files and identify the smallest observable behavior that
could fail. Then use the applicable layers below. A passing lower layer does not
replace a relevant higher layer.

## Apply the workflow during review

Treat execution evidence as part of correctness when reviewing the package, a
branch, or a pull request:

- Map the changed files to the affected behavior and the relevant row in the
  preparation table below. Review the implementation and its validation
  together.
- Run safe focused local code and offline tests when the supplied evidence is
  absent, stale, skipped, or does not execute the changed path.
- When a change crosses Fabric, verify that matching live evidence exists. If
  the review task authorizes using the persistent sandbox, run the narrow
  filter yourself. Otherwise report the missing live evidence and the exact
  runner command required; do not describe that surface as verified.
- Distinguish an implementation defect, missing execution evidence, and an
  external Fabric blocker in review findings. A pending CI run is not evidence
  that the code works.

Do not modify implementation code merely because review found an issue unless
the task also asks for fixes.

### 1. Execute package code locally

Load the checkout instead of an installed release and call the changed path:

```powershell
Rscript -e "devtools::load_all(); <focused expression>"
```

Run the nearest unit test file or description during implementation:

```powershell
Rscript -e "devtools::test_active_file('R/<file>.R')"
Rscript -e "devtools::test_active_file('R/<file>.R', desc = '<test name>')"
```

For sandbox Python changes, run the matching test module:

```powershell
uv --directory tools/fabric-sandbox run pytest tests/<test_file>.py -q
```

Use the broader offline suites after the focused loop is green:

```powershell
Rscript -e "devtools::test(stop_on_failure = TRUE)"
uv --directory tools/fabric-sandbox run pytest -q
```

### 2. Cross the live Fabric boundary when the change does

Use the persistent runner for code that depends on Fabric authentication,
request/response behavior, hosted execution, item definitions, SQL/KQL/DAX/
GraphQL/Livy endpoints, OneLake, or job state. Run from the repository root:

```r
source("tools/fabric-sandbox/local-integration.R")
run_fabric_integration_tests(filter = "integration-fabric-<feature>")
```

The runner loads the current checkout, authenticates with the configured client
secret or cached interactive identity, verifies the marked workspace, refreshes
the live manifest, makes integration tests required rather than skipped, and
runs the requested testthat filter.

When an item definition under `infra/fabric/workspace/` changed, deploy that
exact item in the same run before testing it:

```r
run_fabric_integration_tests(
  filter = "integration-fabric-jobs",
  deploy_items = c(
    "JobFixtures.Notebook",
    "TestPipeline.DataPipeline",
    "TestSparkJob.SparkJobDefinition"
  )
)
```

`deploy_items` entries must exactly match repository directory names in
`Name.Type` form. Include dependencies that also changed. Selective deployment
can add a new source-controlled Fabric item to the existing persistent
workspace; it does not require provisioning a new workspace.

For job changes that also need fresh Spark fixtures, keep the jobs filter and
request seeding. The runner automatically invokes `seed --scope jobs` and
`discover --scope jobs`:

```r
run_fabric_integration_tests(
  filter = "integration-fabric-jobs",
  deploy_items = c(
    "SeedFixtures.Notebook",
    "JobFixtures.Notebook",
    "TestPipeline.DataPipeline",
    "TestSparkJob.SparkJobDefinition"
  ),
  seed_fixtures = TRUE
)
```

This path authenticates only to Fabric and OneLake, runs the Spark seed stage,
and writes a jobs-specific manifest. Do not replace it with a full seed merely
because another persistent-sandbox service is delayed.

When `SeedFixtures.Notebook` or `infra/fabric/fixtures/` changes, publish the
seed notebook and explicitly refresh fixtures:

```r
run_fabric_integration_tests(
  filter = "integration-fabric-onelake",
  deploy_items = "SeedFixtures.Notebook",
  seed_fixtures = TRUE
)
```

Do not set `seed_fixtures = TRUE` for ordinary R or item-definition changes.

For an ad hoc live call that is more diagnostic than a test group, connect the
playground, load the checkout, and invoke the changed public function against a
discovered target:

```r
source("playground/sandbox.R")
sandbox <- connect_playground_sandbox()
devtools::load_all()
<focused call using sandbox$workspace or sandbox$targets>
```

Convert a useful reproduction into a focused integration test when it guards a
regression.

### 3. Match repository changes to live preparation

| Change | Persistent preparation | Minimum live evidence |
| --- | --- | --- |
| R API behavior only | None | Matching `integration-fabric-<feature>` filter or a focused playground call |
| Notebook, pipeline, Spark job, semantic model, or Environment definition | Select exact `Name.Type` item(s) with `deploy_items` | Test the deployed item through the matching package API |
| Seed notebook or fixture files used by job tests | Deploy changed job items; use the jobs filter and set `seed_fixtures = TRUE` | `integration-fabric-jobs` against the jobs-scoped manifest |
| Seed notebook or fixture files used by other data services | Deploy `SeedFixtures.Notebook`; set `seed_fixtures = TRUE` | Narrow data feature filter that reads the refreshed fixture |
| Sandbox deploy/discover/seed code | Focused Python tests first | Exercise the affected command plus a narrow R integration filter |
| Terraform-managed workspace resources | `terraform fmt -check` and `terraform validate` locally | Rebuild only when structural live validation is necessary and authorized |

Useful filters are the filenames without `test-` and `.R`, including
`integration-fabric-auth-discovery`, `integration-fabric-jobs`,
`integration-fabric-kql-graphql`, `integration-fabric-livy`,
`integration-fabric-onelake`, `integration-fabric-onelake-tables`,
`integration-fabric-power-bi`, `integration-fabric-runtime-compatibility`, and
`integration-fabric-sql`.

## Finish gate

Before reporting an executable change complete or a review passed:

1. Run the changed code path, not only a parser, linter, or mock.
2. Run the smallest relevant offline tests and then the appropriate broader
   offline suite for the touched component.
3. If the behavior crosses Fabric, credentials plus the persistent sandbox are
   available, and the task authorizes live execution, run matching live evidence
   during this task.
4. Report the exact commands or filters run and distinguish passes, skips, and
   untested surfaces. A skipped integration test is not live evidence.

Do not use “CI will catch it,” “the change is small,” or “provisioning is slow”
as reasons to omit executable evidence. If the persistent path is genuinely
blocked, record the exact failing command and blocker after one targeted retry
for a transient service error, and state what remains unverified.
