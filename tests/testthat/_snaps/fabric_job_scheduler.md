# schedule boundaries reject fractional seconds explicitly

    Code
      fabric_job_schedule_config("Daily", start_time = value, end_time = "2027-01-01T00:00:00Z",
        times = "09:00")
    Condition
      Error in `.fabric_schedule_utc()`:
      ! `start_time` must have whole-second precision; round or truncate explicitly before submission

---

    Code
      fabric_job_schedule_config("Daily", start_time = value, end_time = "2027-01-01T00:00:00Z",
        times = "09:00")
    Condition
      Error in `.fabric_schedule_utc()`:
      ! `start_time` must have whole-second precision; round or truncate explicitly before submission

---

    Code
      fabric_job_schedule_config("Daily", start_time = value, end_time = "2027-01-01T00:00:00Z",
        times = "09:00")
    Condition
      Error in `.fabric_schedule_utc()`:
      ! `start_time` must have whole-second precision; round or truncate explicitly before submission

---

    Code
      fabric_job_schedule_config("Daily", start_time = value, end_time = "2027-01-01T00:00:00Z",
        times = "09:00")
    Condition
      Error in `.fabric_schedule_utc()`:
      ! `start_time` must have whole-second precision; round or truncate explicitly before submission

---

    Code
      .fabric_schedule_utc(timestamp, "end_time")
    Condition
      Error in `.fabric_schedule_utc()`:
      ! `end_time` must have whole-second precision; round or truncate explicitly before submission

# semantic model schedules require the Power BI dataset API

    Code
      fabric_job_schedules(scheduler_test_item("SemanticModel"), token = "test-token")
    Condition
      Error in `.fabric_job_schedule_type()`:
      ! Semantic-model refresh schedules use the Power BI dataset API, not the Fabric Core Job Scheduler

# schedule JSON preservation respects escaping and member boundaries

    Code
      .fabric_job_json_member("{\"executionData\":1,\"execution\\u0044ata\":2}",
        "executionData")
    Condition
      Error in `.fabric_job_json_member()`:
      ! Fabric returned duplicate executionData fields

---

    Code
      .fabric_job_json_member("{\"executionData\":", "executionData")
    Condition
      Error in `.fabric_job_json_member()`:
      ! Fabric returned malformed schedule JSON

