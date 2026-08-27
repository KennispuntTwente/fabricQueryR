# semantic model schedules require the Power BI dataset API

    Code
      fabric_job_schedules(scheduler_test_item("SemanticModel"), token = "test-token")
    Condition
      Error in `.fabric_job_schedule_type()`:
      ! Semantic-model refresh schedules use the Power BI dataset API, not the Fabric Core Job Scheduler

