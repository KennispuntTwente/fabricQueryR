# Schedule execution evidence

Set `FABRIC_TEST_REQUIRED_FEATURES=nonutc-schedules` and run
`integration-fabric-jobs-schedules-nonutc` to require a Daily notebook schedule to
fire with a unique marker at the intended UTC instant. The test uses the Windows
zone `W. Europe Standard Time`, a clock time derived independently using
`Europe/Amsterdam`, and UTC schedule boundaries. It allows ten minutes of service
start delay and twenty minutes for completion, then deletes its schedule.
The request follows Microsoft's
[schedule contract](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/create-item-schedule).

On 2026-09-16 the persistent development workspace accepted this configuration
but produced no scheduled execution in either of two twenty-minute attempts.
Consequently non-UTC firing remains unresolved; passing schedule CRUD is not
evidence that it fires. The required feature keeps this failure visible when
selected. No undocumented time-zone offset adjustment was added to the client.

The same file checks a disabled Spark job schedule through create, list, update,
and delete without requiring the timing feature. Existing jobs tests cover
Notebook and Pipeline schedule routes and UTC Cron firing.

## Additional workload routes

Set `FABRIC_TEST_REQUIRED_FEATURES=workload-schedules` and supply
`FABRIC_TEST_WORKLOAD_SCHEDULES_JSON` with these entries:

```json
{
  "dataflow-execute": {"workspace": "<guid>", "id": "<Dataflow guid>"},
  "dataflow-publish": {"workspace": "<guid>", "id": "<Dataflow guid>"},
  "lakehouse-mlv": {
    "workspace": "<guid>", "id": "<Lakehouse guid>",
    "execution_data": {"mlvExecutionDefinitionId": "<definition guid>"}
  },
  "dbt": {"workspace": "<guid>", "id": "<DataBuildToolJob guid>"}
}
```

Run `integration-fabric-jobs-schedule-workloads`. Each case creates a disabled
Daily schedule, lists it, changes its clock time, and deletes it. Supply any
additional execution data required by the configured workload. These tests do
not execute or publish the Dataflow, refresh materialized views, or run dbt.
Missing entries fail a required run and are explicit skips otherwise. Use
dedicated test items that the runner can schedule.
