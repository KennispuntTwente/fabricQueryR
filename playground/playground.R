devtools::load_all()

library(dplyr)
library(purrr)

# options(fabricqueryr.sql.driver = "ODBC Driver 17 for SQL Server")
# Sys.setenv(FABRICQUERYR_TENANT_ID = "...")
# Sys.setenv(FABRICQUERYR_CLIENT_ID = "...")

# SQL connection ----------------------------------------------------------

# Get connection
con <- fabric_sql_connect(
  server = "2gxzdezjoe4ethsnmm6grd4tya-v7qcb4ufxtxebbnrg2fuox4qiy.datawarehouse.fabric.microsoft.com"
)

# List databases
DBI::dbGetQuery(con, "SELECT name FROM sys.databases")

# List tables in the current database
DBI::dbGetQuery(
  con,
  "
  SELECT TABLE_SCHEMA, TABLE_NAME
  FROM INFORMATION_SCHEMA.TABLES
  WHERE TABLE_TYPE = 'BASE TABLE'
  "
)

# Read 'Patienten' table
df_sql <- DBI::dbReadTable(con, "Patienten")

# Close connection
DBI::dbDisconnect(con)


# OneLake table -----------------------------------------------------------

df_onelake <- fabric_onelake_read_delta_table(
  table_path = "Patienten/patienten_hash",
  workspace_name = "Borne_automatisering_test",
  lakehouse_name = "Lakehouse.Lakehouse",
)


# DAX query ---------------------------------------------------------------

df_dax <- fabric_pbi_dax_query(
  connstr = "Data Source=powerbi://api.powerbi.com/v1.0/myorg/Borne_automatisering_test;Initial Catalog=test data 1;",
  dax = "EVALUATE TOPN(100000, 'Sheet1')"
)


# Livy API query ----------------------------------------------------------

sess_url <- "https://api.fabric.microsoft.com/v1/workspaces/f220e0af-bc85-40ee-85b1-368b475f9046/lakehouses/44de17dc-2d5e-44c6-bdce-6bcbc999e01e/livyapi/versions/2023-12-01/sessions"

# Livy API can run SQL, SparkR, PySpark, & Spark
# Below are two examples of SQL & SparkR usage

## 1 Livy & SQL

# Here we run SQL remotely in Microsoft Fabric with Spark, to get data to local R
# Since Livy API cannot directly give back a proper DF, we build it from returned schema & matrix

# Run Livy SQL query
livy_sql_result <- fabric_livy_run(
  livy_url = sess_url,
  kind = "sql",
  code = "SELECT * FROM Patienten LIMIT 1000"
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
livy_sparkr_result <- fabric_livy_run(
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
  )
)

# Extract marked B64 string from Livy output
txt <- livy_sparkr_result$output$data$`text/plain`
b64 <- sub('.*<<B64RDS>>', '', txt)
b64 <- sub('<<END>>.*', '', b64)

# Decode to dataframe
raw_gz <- base64enc::base64decode(b64)
r_raw  <- memDecompress(raw_gz, type = "gzip")
df_livy_sparkr <- unserialize(r_raw)
