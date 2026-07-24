"""Deterministic Fabric API for GraphQL fixture definition."""

from __future__ import annotations

from typing import Any


GRAPHQL_API_NAME = "TestGraphQL"
GRAPHQL_TYPE = "fabricqueryr_basic"
GRAPHQL_ROOT_FIELD = "fabricqueryr_basics"
GRAPHQL_SOURCE_OBJECT = "dbo.fabricqueryr_basic"


def graphql_definition(
    workspace_id: str,
    sql_endpoint_id: str,
) -> dict[str, Any]:
    """Build the supported public definition over the seeded lakehouse table."""
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
                "sourceItemId": sql_endpoint_id,
                "sourceWorkspaceId": workspace_id,
                "sourceType": "SqlAnalyticsEndpoint",
                "objects": [
                    {
                        "graphqlType": GRAPHQL_TYPE,
                        "sourceObject": GRAPHQL_SOURCE_OBJECT,
                        "sourceObjectType": "Table",
                        "actions": {"Query": "Enabled"},
                        "fieldMappings": fields,
                        "relationships": [],
                    }
                ],
            }
        ],
    }
