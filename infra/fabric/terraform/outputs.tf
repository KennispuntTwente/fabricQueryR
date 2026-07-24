output "workspace_id" {
  description = "ID consumed by fabric-cicd and the integration tests."
  value       = fabric_workspace.sandbox.id
}

output "workspace_name" {
  description = "Display name of the ephemeral workspace."
  value       = fabric_workspace.sandbox.display_name
}

output "onelake_dfs_endpoint" {
  description = "Workspace OneLake DFS endpoint."
  value       = fabric_workspace.sandbox.onelake_endpoints.dfs_endpoint
}

output "onelake_blob_endpoint" {
  description = "Workspace OneLake Blob endpoint."
  value       = fabric_workspace.sandbox.onelake_endpoints.blob_endpoint
}

output "lakehouse_id" {
  description = "ID used to bind the seed notebook and upload fixtures."
  value       = fabric_lakehouse.test.id
}

output "warehouse_id" {
  description = "ID of the Warehouse exercised by SQL integration tests."
  value       = fabric_warehouse.test.id
}

output "sql_database_id" {
  description = "ID of the SQL Database exercised by SQL integration tests."
  value       = fabric_sql_database.test.id
}
