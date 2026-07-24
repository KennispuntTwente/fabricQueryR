resource "fabric_workspace" "sandbox" {
  display_name = var.workspace_name
  description  = var.workspace_description
  capacity_id  = var.capacity_id

  timeouts = {
    create = "15m"
    update = "15m"
    delete = "15m"
  }
}

resource "fabric_lakehouse" "test" {
  display_name = "TestLakehouse"
  description  = "Ephemeral integration-test lakehouse for fabricQueryR"
  workspace_id = fabric_workspace.sandbox.id

  configuration = {
    enable_schemas = true
  }

  timeouts = {
    create = "15m"
    update = "15m"
    delete = "15m"
  }
}

resource "fabric_warehouse" "test" {
  display_name = "TestWarehouse"
  description  = "Ephemeral integration-test warehouse for fabricQueryR"
  workspace_id = fabric_workspace.sandbox.id

  configuration = {
    collation_type = "Latin1_General_100_BIN2_UTF8"
  }

  timeouts = {
    create = "20m"
    update = "15m"
    delete = "15m"
  }
}

resource "fabric_sql_database" "test" {
  display_name = "TestSQLDatabase"
  description  = "Ephemeral integration-test SQL database for fabricQueryR"
  workspace_id = fabric_workspace.sandbox.id

  configuration = {
    creation_mode         = "New"
    backup_retention_days = 1
  }

  timeouts = {
    create = "20m"
    update = "20m"
    delete = "20m"
  }
}

resource "fabric_workspace_role_assignment" "test_principal" {
  count = var.test_principal_id == null ? 0 : 1

  workspace_id = fabric_workspace.sandbox.id
  principal = {
    id   = var.test_principal_id
    type = "ServicePrincipal"
  }
  role = var.test_principal_role
}
