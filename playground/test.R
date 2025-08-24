devtools::load_all()

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
