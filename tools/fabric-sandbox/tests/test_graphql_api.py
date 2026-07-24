from fabricqueryr_sandbox.graphql_api import (
    GRAPHQL_SOURCE_OBJECT,
    GRAPHQL_TYPE,
    graphql_definition,
)


def test_graphql_definition_exposes_the_seeded_table_read_only():
    definition = graphql_definition("workspace-id", "sql-endpoint-id")

    assert definition["$schema"].endswith(
        "/fabric/item/graphqlApi/definition/1.0.0/schema.json"
    )
    assert len(definition["datasources"]) == 1
    source = definition["datasources"][0]
    assert source["sourceWorkspaceId"] == "workspace-id"
    assert source["sourceItemId"] == "sql-endpoint-id"
    assert source["sourceType"] == "SqlAnalyticsEndpoint"
    assert len(source["objects"]) == 1
    exposed = source["objects"][0]
    assert exposed["graphqlType"] == GRAPHQL_TYPE
    assert exposed["sourceObject"] == GRAPHQL_SOURCE_OBJECT
    assert exposed["sourceObjectType"] == "Table"
    assert exposed["actions"] == {"Query": "Enabled"}
    assert set(exposed["fieldMappings"]) == {
        "id",
        "name",
        "category",
        "amount",
        "loaded_at",
    }
