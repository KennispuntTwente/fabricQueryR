"""Deterministic Fabric API for GraphQL fixture definition."""

from __future__ import annotations

from typing import Any


GRAPHQL_API_NAME = "TestGraphQL"
GRAPHQL_TYPE = "fabricqueryr_basic"
GRAPHQL_ROOT_FIELD = "fabricqueryr_basics"
GRAPHQL_CREATE_FIELD = "createfabricqueryr_basic"
GRAPHQL_SOURCE_OBJECT = "dbo.fabricqueryr_sql_types"


def graphql_definition(
    workspace_id: str,
    warehouse_id: str,
) -> dict[str, Any]:
    """Build the public definition over the writable Warehouse fixture."""
    fields = {
        "id": "id",
        "name": "name",
        "category": "category",
        "amount": "amount",
        "loaded_at": "loaded_at",
    }
    return {
        "$schema": (
            "https://developer.microsoft.com/json-schemas/fabric/item/"
            "graphqlApi/definition/1.0.0/schema.json"
        ),
        "datasources": [
            {
                "sourceItemId": warehouse_id,
                "sourceWorkspaceId": workspace_id,
                "sourceType": "Warehouse",
                "objects": [
                    {
                        "graphqlType": GRAPHQL_TYPE,
                        "sourceObject": GRAPHQL_SOURCE_OBJECT,
                        "sourceObjectType": "Table",
                        "actions": {
                            "Query": "Enabled",
                            "Create": "Enabled",
                        },
                        "fieldMappings": fields,
                        "relationships": [],
                    }
                ],
            }
        ],
    }
