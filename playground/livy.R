devtools::load_all()

# sessions flow (interactive)
sess_url <- "https://api.fabric.microsoft.com/v1/workspaces/f220e0af-bc85-40ee-85b1-368b475f9046/lakehouses/44de17dc-2d5e-44c6-bdce-6bcbc999e01e/livyapi/versions/2023-12-01/sessions"

# run sparkr code
res <- fabric_livy_run(
  livy_url = sess_url,
  kind = "pyspark",
  code = "print(1+2)"
)
print(res$output$parsed) # "3" (text/plain)

print(res)
