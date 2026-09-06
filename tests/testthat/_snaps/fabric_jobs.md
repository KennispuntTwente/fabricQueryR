# unsafe integer64 job parameters fail before any request

    Code
      .fabric_job_parameters(list(value = bit64::as.integer64("9007199254740993")))
    Condition
      Error in `.fabric_job_parameter()`:
      ! Number parameter `value` cannot represent this integer64 exactly; pass as.character(value) with type Text

---

    Code
      .fabric_job_parameters(list(list(name = "value", value = bit64::as.integer64(
        "9223372036854775807"), type = "Number")))
    Condition
      Error in `.fabric_job_parameter()`:
      ! Number parameter `value` cannot represent this integer64 exactly; pass as.character(value) with type Text

# integer64 job parameters retain scalar and explicit integer validation

    Code
      .fabric_job_parameter("value", bit64::as.integer64("2147483648"), "Integer")
    Condition
      Error in `.fabric_job_parameter()`:
      ! Integer parameter `value` must be a whole 32-bit number

---

    Code
      .fabric_job_parameters(list(value = bit64::as.integer64(NA)))
    Condition
      Error in `.fabric_job_parameter()`:
      ! Fabric parameter `value` must be one non-missing scalar

---

    Code
      .fabric_job_parameters(list(value = bit64::as.integer64(c("1", "2"))))
    Condition
      Error in `.fabric_job_parameter()`:
      ! Fabric parameter `value` must be one non-missing scalar

