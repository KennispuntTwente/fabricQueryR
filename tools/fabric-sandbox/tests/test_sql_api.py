import struct

import pytest

from fabricqueryr_sandbox.sql_api import (
    SQL_COPT_SS_ACCESS_TOKEN,
    _odbc_access_token,
    _sql_target,
    seed_sql_fixture,
)


class FakeCursor:
    def __init__(self):
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)
        return self

    def fetchone(self):
        return (3, 30.5)


class FakeConnection:
    def __init__(self):
        self.cursor_value = FakeCursor()
        self.closed = False

    def cursor(self):
        return self.cursor_value

    def close(self):
        self.closed = True


def test_sql_target_accepts_bare_and_portal_connection_strings():
    assert _sql_target("warehouse.sql.test", "TestWarehouse") == (
        "warehouse.sql.test",
        "TestWarehouse",
    )
    assert _sql_target(
        (
            "Server=tcp:database.sql.test,1433;"
            "Initial Catalog=internal-name;"
            "Encrypt=True"
        ),
        "authoritative-name",
    ) == ("database.sql.test,1433", "authoritative-name")

    with pytest.raises(ValueError, match="Server/Data Source"):
        _sql_target("Encrypt=True", "database")


def test_odbc_access_token_has_length_prefixed_utf16_payload():
    token = _odbc_access_token("abc")

    assert struct.unpack("<I", token[:4])[0] == 6
    assert token[4:] == b"a\x00b\x00c\x00"


def test_seed_sql_fixture_creates_verifies_and_closes_typed_table():
    connection = FakeConnection()
    calls = []

    def connect(connection_string, **kwargs):
        calls.append((connection_string, kwargs))
        return connection

    seed_sql_fixture(
        "Server=tcp:warehouse.sql.test,1433;Encrypt=True",
        "TestWarehouse",
        "secret-token",
        connect=connect,
        attempts=1,
    )

    assert len(calls) == 1
    connection_string, kwargs = calls[0]
    assert "Server=warehouse.sql.test,1433" in connection_string
    assert "Database=TestWarehouse" in connection_string
    assert kwargs["autocommit"] is True
    assert set(kwargs["attrs_before"]) == {SQL_COPT_SS_ACCESS_TOKEN}
    assert b"secret-token" not in kwargs["attrs_before"][
        SQL_COPT_SS_ACCESS_TOKEN
    ]
    assert connection.cursor_value.statements[0].startswith(
        "DROP TABLE IF EXISTS dbo.fabricqueryr_sql_types"
    )
    assert connection.cursor_value.statements[1].startswith(
        "CREATE TABLE dbo.fabricqueryr_sql_types"
    )
    assert "INSERT INTO dbo.fabricqueryr_sql_types" in (
        connection.cursor_value.statements[2]
    )
    assert connection.cursor_value.statements[3].startswith(
        "SELECT COUNT(*), SUM(amount)"
    )
    assert connection.closed is True


def test_seed_sql_fixture_can_publish_warehouse_mutations():
    connection = FakeConnection()

    seed_sql_fixture(
        "warehouse.sql.test",
        "TestWarehouse",
        "secret-token",
        connect=lambda *args, **kwargs: connection,
        attempts=1,
        mutate=True,
    )

    statements = connection.cursor_value.statements
    assert statements[3] == (
        "DROP TABLE IF EXISTS dbo.fabricqueryr_sql_mutations"
    )
    assert statements[4].startswith(
        "CREATE TABLE dbo.fabricqueryr_sql_mutations"
    )
    assert statements[5].startswith(
        "INSERT INTO dbo.fabricqueryr_sql_mutations"
    )
    assert statements[6].startswith(
        "UPDATE dbo.fabricqueryr_sql_mutations"
    )
    assert statements[7] == (
        "DELETE FROM dbo.fabricqueryr_sql_mutations WHERE id = 1"
    )
    assert "alpha-replacement" in statements[8]
    assert statements[9].startswith("SELECT COUNT(*), SUM(amount)")
    assert statements[10].startswith("SELECT COUNT(*), SUM(amount)")
