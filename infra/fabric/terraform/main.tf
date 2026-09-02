locals {
  provision_full_fixture = var.fixture_scope == "all"
}

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

resource "fabric_lakehouse" "non_schema" {
  display_name = "TestLakehouseNoSchemas"
  description  = "Ephemeral schema-disabled lakehouse for fabricQueryR"
  workspace_id = fabric_workspace.sandbox.id

  configuration = {
    enable_schemas = false
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

resource "fabric_warehouse_snapshot" "test" {
  count = local.provision_full_fixture ? 1 : 0

  display_name = "TestWarehouseSnapshot"
  description  = "Ephemeral warehouse snapshot for fabricQueryR integration tests"
  workspace_id = fabric_workspace.sandbox.id

  configuration = {
    parent_warehouse_id = fabric_warehouse.test.id
  }

  timeouts = {
    create = "20m"
    update = "15m"
    delete = "15m"
  }
}

resource "fabric_sql_database" "test" {
  count = local.provision_full_fixture ? 1 : 0

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

resource "fabric_mirrored_database" "test" {
  display_name = "TestMirroredDatabase"
  description  = "Ephemeral open mirrored database for fabricQueryR integration tests"
  workspace_id = fabric_workspace.sandbox.id
  format       = "Default"

  definition = {
    "mirroring.json" = {
      source          = "${path.module}/definitions/open-mirroring.json"
      processing_mode = "None"
    }
  }

  timeouts = {
    create = "20m"
    update = "15m"
    delete = "20m"
  }
}

resource "fabric_eventhouse" "test" {
  count = local.provision_full_fixture ? 1 : 0

  display_name = "TestEventhouse"
  description  = "Ephemeral integration-test Eventhouse for fabricQueryR"
  workspace_id = fabric_workspace.sandbox.id

  configuration = {
    minimum_consumption_units = 0
  }

  timeouts = {
    create = "20m"
    update = "15m"
    delete = "20m"
  }
}

resource "fabric_kql_database" "test" {
  count = local.provision_full_fixture ? 1 : 0

  display_name = "TestKQLDatabase"
  description  = "Ephemeral integration-test KQL database for fabricQueryR"
  workspace_id = fabric_workspace.sandbox.id

  configuration = {
    database_type = "ReadWrite"
    eventhouse_id = fabric_eventhouse.test[0].id
  }

  timeouts = {
    create = "20m"
    update = "15m"
    delete = "20m"
  }
}

resource "fabric_graphql_api" "test" {
  count = local.provision_full_fixture ? 1 : 0

  display_name = "TestGraphQL"
  description  = "Ephemeral GraphQL API for fabricQueryR integration tests"
  workspace_id = fabric_workspace.sandbox.id

  timeouts = {
    create = "15m"
    update = "15m"
    delete = "15m"
  }
}

resource "fabric_workspace_role_assignment" "test_principal" {
  count = var.test_principal_id == null ? 0 : 1

  workspace_id = fabric_workspace.sandbox.id
  principal = {
    id   = var.test_principal_id
    type = var.test_principal_type
  }
  role = var.test_principal_role
}
