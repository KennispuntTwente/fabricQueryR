from fabricqueryr_sandbox.graphql_api import (
    GRAPHQL_CREATE_FIELD,
    GRAPHQL_ROOT_FIELD,
    GRAPHQL_SOURCE_OBJECT,
    GRAPHQL_TYPE,
    graphql_definition,
)


def test_graphql_definition_exposes_the_writable_warehouse_table():
    definition = graphql_definition("workspace-id", "warehouse-id")

    assert definition["$schema"].endswith(
        "/fabric/item/graphqlApi/definition/1.0.0/schema.json"
    )
    assert len(definition["datasources"]) == 1
    source = definition["datasources"][0]
    assert source["sourceWorkspaceId"] == "workspace-id"
    assert source["sourceItemId"] == "warehouse-id"
    assert source["sourceType"] == "Warehouse"
    assert len(source["objects"]) == 1
    exposed = source["objects"][0]
    assert exposed["graphqlType"] == GRAPHQL_TYPE
    assert exposed["sourceObject"] == GRAPHQL_SOURCE_OBJECT
    assert exposed["sourceObjectType"] == "Table"
    assert exposed["actions"] == {
        "Query": "Enabled",
        "Create": "Enabled",
    }
    assert set(exposed["fieldMappings"]) == {
        "id",
        "name",
        "category",
        "amount",
        "loaded_at",
    }
    assert GRAPHQL_ROOT_FIELD == "fabricqueryr_basics"
    assert GRAPHQL_CREATE_FIELD == "createfabricqueryr_basic"
