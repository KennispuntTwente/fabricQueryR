# Find your session URL in Fabric by going to a 'Lakehouse' item,
#   then go to 'Settings' -> 'Livy Endpoint' -> 'Session job connection string'
sess_url <- "https://api.fabric.microsoft.com/v1/workspaces/.../lakehouses/.../livyapi/..."

# Livy API can run SQL, SparkR, PySpark, & Spark
# Below are examples of 1) SQL & 2) SparkR usage

# Example is not executed since it requires configured credentials for Fabric
\dontrun{
## 1 Livy & SQL

# Here we run SQL remotely in Microsoft Fabric with Spark, to get data to local R
# Since Livy API cannot directly give back a proper DF, we build it from returned schema & matrix

# Run Livy SQL query
livy_sql_result <- fabric_livy_query(
  livy_url = sess_url,
  kind = "sql",
  code = "SELECT * FROM Patienten LIMIT 1000",
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)

# '$schema$fields' contains column info, & '$data' contains data as matrix without column names
payload <- livy_sql_result$output$data[["application/json"]]
schema  <- as_tibble(payload$schema$fields) # has columns: name, type, nullable
col_nms <- schema$name

# Build dataframe (tibble) from the Livy result
df_livy_sql <- payload$data |>
  as_tibble(.name_repair = "minimal") |>
  set_names(col_nms) |>
  mutate(
    # cast by schema$type (add more cases if your schema includes them)
    across(all_of(schema$name[schema$type == "long"]),    readr::parse_integer),
    across(all_of(schema$name[schema$type == "double"]),  readr::parse_double),
    across(all_of(schema$name[schema$type == "boolean"]), readr::parse_logical),
    across(all_of(schema$name[schema$type == "string"]),  as.character)
  )

## 2 Livy & SparkR

# Here we run R code remotely in Microsoft Fabric with SparkR, to get data to local R
# Since Livy API cannot directly give back a proper DF, we encode/decode B64 in SparkR/local R

# Run Livy SparkR query
livy_sparkr_result <- fabric_livy_query(
  livy_url = sess_url,
  kind = "sparkr",
  code = paste(
    # Obtain data in remote R (SparkR)
    'library(SparkR); library(base64enc)',
    'df <- sql("SELECT * FROM Patienten") |> limit(1000L) |> collect()',

    # serialize -> gzip -> base64
    'r_raw <- serialize(df, connection = NULL)',
    'raw_gz <- memCompress(r_raw, type = "gzip")',
    'b64 <- base64enc::base64encode(raw_gz)',

    # output marked B64 string
    'cat("<<B64RDS>>", b64, "<<END>>", sep = "")',
    sep = "\n"
  ),
  tenant_id = Sys.getenv("FABRICQUERYR_TENANT_ID"),
  client_id = Sys.getenv("FABRICQUERYR_CLIENT_ID")
)

# Extract marked B64 string from Livy output
txt <- livy_sparkr_result$output$data$`text/plain`
b64 <- sub('.*<<B64RDS>>', '', txt)
b64 <- sub('<<END>>.*', '', b64)

# Decode to dataframe
raw_gz <- base64enc::base64decode(b64)
r_raw  <- memDecompress(raw_gz, type = "gzip")
df_livy_sparkr <- unserialize(r_raw)
}
