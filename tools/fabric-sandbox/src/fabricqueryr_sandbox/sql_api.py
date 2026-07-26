"""Seed deterministic fixtures through Fabric's SQL endpoints."""

from __future__ import annotations

import struct
import time
from collections.abc import Callable

import pyodbc


SQL_AUDIENCE = "https://database.windows.net/.default"
SQL_COPT_SS_ACCESS_TOKEN = 1256
SQL_FIXTURE_TABLE = "fabricqueryr_sql_types"


def _sql_target(connection_string: str, database: str) -> tuple[str, str]:
    """Extract a server while using Fabric's authoritative database name."""
    fields: dict[str, str] = {}
    bare: list[str] = []
    for token in connection_string.split(";"):
        token = token.strip()
        if not token:
            continue
        if "=" not in token:
            bare.append(token)
            continue
        key, value = token.split("=", 1)
        fields[key.strip().lower().replace(" ", "")] = value.strip()

    server = next(
        (
            fields[key]
            for key in (
                "server",
                "datasource",
                "address",
                "addr",
                "networkaddress",
            )
            if key in fields
        ),
        None,
    )
    if server is None:
        if len(bare) != 1:
            raise ValueError(
                "SQL fixture target must contain one Server/Data Source"
            )
        server = bare[0]
    if server.lower().startswith("tcp:"):
        server = server[4:].strip()
    if not server or not database.strip():
        raise ValueError("SQL fixture server and database must be non-empty")
    return server, database.strip()


def _odbc_access_token(token: str) -> bytes:
    """Build the ODBC ACCESSTOKEN structure expected by Driver 18."""
    encoded = token.encode("utf-16-le")
    return struct.pack(f"<I{len(encoded)}s", len(encoded), encoded)


def seed_sql_fixture(
    connection_string: str,
    database: str,
    token: str,
    *,
    connect: Callable[..., pyodbc.Connection] | None = None,
    attempts: int = 30,
    retry_delay: float = 10,
) -> None:
    """Create a small typed table in a Warehouse or SQL Database."""
    if attempts < 1:
        raise ValueError("attempts must be positive")
    server, database = _sql_target(connection_string, database)
    connect = connect or pyodbc.connect
    odbc_connection_string = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={server};"
        f"Database={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "MARS_Connection=no;"
        "Connection Timeout=30"
    )
    attributes = {
        SQL_COPT_SS_ACCESS_TOKEN: _odbc_access_token(token),
    }
    statements = (
        f"DROP TABLE IF EXISTS dbo.{SQL_FIXTURE_TABLE}",
        (
            f"CREATE TABLE dbo.{SQL_FIXTURE_TABLE} ("
            "id int NOT NULL, "
            "name varchar(100) NOT NULL, "
            "category varchar(20) NOT NULL, "
            "amount decimal(10, 2) NULL, "
            "active bit NULL, "
            "event_date date NULL, "
            "loaded_at datetime2(0) NOT NULL, "
            "nullable_value varchar(20) NULL"
            ")"
        ),
        (
            f"INSERT INTO dbo.{SQL_FIXTURE_TABLE} "
            "SELECT 1, 'alpha', 'A', CAST(10.50 AS decimal(10, 2)), 1, "
            "CAST('2026-01-01' AS date), "
            "CAST('2026-01-01T00:00:00' AS datetime2(0)), NULL "
            "UNION ALL "
            "SELECT 2, 'beta', 'B', CAST(20.00 AS decimal(10, 2)), 0, "
            "CAST('2026-01-02' AS date), "
            "CAST('2026-01-01T00:00:00' AS datetime2(0)), 'present' "
            "UNION ALL "
            "SELECT 3, 'gamma', 'A', NULL, NULL, NULL, "
            "CAST('2026-01-01T00:00:00' AS datetime2(0)), NULL"
        ),
    )

    last_error: pyodbc.Error | None = None
    for attempt in range(1, attempts + 1):
        connection = None
        try:
            connection = connect(
                odbc_connection_string,
                attrs_before=attributes,
                autocommit=True,
            )
            cursor = connection.cursor()
            for statement in statements:
                cursor.execute(statement)
            row = cursor.execute(
                f"SELECT COUNT(*), SUM(amount) "
                f"FROM dbo.{SQL_FIXTURE_TABLE}"
            ).fetchone()
            if row is None or row[0] != 3 or float(row[1]) != 30.5:
                raise RuntimeError(
                    f"SQL fixture verification failed for {database}"
                )
            return
        except pyodbc.Error as error:
            last_error = error
            if attempt == attempts:
                break
            time.sleep(retry_delay)
        finally:
            if connection is not None:
                connection.close()
    raise RuntimeError(
        f"SQL endpoint for {database} was not ready after {attempts} attempts"
    ) from last_error
