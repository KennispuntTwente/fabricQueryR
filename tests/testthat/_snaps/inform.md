# condition helpers use cli markup and rlang conditions

    Code
      .fabric_warn(c("Could not resolve {.arg {argument}}", i = "Received {.val {value}}"),
      .format = TRUE, class = "fabric_test_warning", call = NULL)
    Condition
      Warning:
      Could not resolve `workspace`
      i Received "missing"

---

    Code
      .fabric_abort(c("Could not resolve {.arg {argument}}", x = "Received {.val {value}}"),
      .format = TRUE, class = "fabric_test_error", detail = 42L, call = NULL)
    Condition
      Error:
      ! Could not resolve `workspace`
      x Received "missing"

# package objects use one cli summary layout

    Code
      .fabric_print("fabric_example", list(id = "item-1", state = "Running", omitted = NULL))
    Message
      <fabric_example>
      id: item-1
      state: Running

