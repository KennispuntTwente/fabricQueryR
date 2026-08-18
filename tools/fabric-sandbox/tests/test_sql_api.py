import struct

import pytest

from fabricqueryr_sandbox.sql_api import (
    SQL_COPT_SS_ACCESS_TOKEN,
    SQL_FIXTURE_VIEW,
    _odbc_access_token,
    _sql_target,
    seed_sql_fixture,
    wait_for_sql_fixture,
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
    assert connection.cursor_value.statements[0] == (
        f"DROP VIEW IF EXISTS dbo.{SQL_FIXTURE_VIEW}"
    )
    assert connection.cursor_value.statements[1].startswith(
        "DROP TABLE IF EXISTS dbo.fabricqueryr_sql_types"
    )
    assert connection.cursor_value.statements[2].startswith(
        "CREATE TABLE dbo.fabricqueryr_sql_types"
    )
    assert "INSERT INTO dbo.fabricqueryr_sql_types" in (
        connection.cursor_value.statements[3]
    )
    assert connection.cursor_value.statements[4].startswith(
        "CREATE VIEW dbo.fabricqueryr_sql_types_view"
    )
    assert connection.cursor_value.statements[5].startswith(
        "SELECT COUNT(*), SUM(amount)"
    )
    assert connection.cursor_value.statements[6].startswith(
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
    assert statements[5] == (
        "DROP TABLE IF EXISTS dbo.fabricqueryr_sql_mutations"
    )
    assert statements[6].startswith(
        "CREATE TABLE dbo.fabricqueryr_sql_mutations"
    )
    assert statements[7].startswith(
        "INSERT INTO dbo.fabricqueryr_sql_mutations"
    )
    assert statements[8].startswith(
        "UPDATE dbo.fabricqueryr_sql_mutations"
    )
    assert statements[9] == (
        "DELETE FROM dbo.fabricqueryr_sql_mutations WHERE id = 1"
    )
    assert "alpha-replacement" in statements[10]
    assert statements[11].startswith("SELECT COUNT(*), SUM(amount)")
    assert statements[12].startswith("SELECT COUNT(*), SUM(amount)")
    assert statements[13].startswith("SELECT COUNT(*), SUM(amount)")


def test_wait_for_sql_fixture_retries_until_rows_are_queryable(monkeypatch):
    first = FakeConnection()
    first.cursor_value.fetchone = lambda: (0, None)
    second = FakeConnection()
    connections = iter([first, second])
    waits = []
    monkeypatch.setattr("fabricqueryr_sandbox.sql_api.time.sleep", waits.append)

    wait_for_sql_fixture(
        "mirrored.sql.test",
        "TestMirroredDatabase",
        "secret-token",
        "fabricqueryr_mirror_types",
        connect=lambda *args, **kwargs: next(connections),
        attempts=2,
        retry_delay=0.25,
    )

    assert waits == [0.25]
    assert first.closed is True
    assert second.closed is True
    assert first.cursor_value.statements == [
        "SELECT COUNT(*), SUM(amount) "
        "FROM dbo.fabricqueryr_mirror_types"
    ]


def test_wait_for_sql_fixture_rejects_unsafe_table_names():
    with pytest.raises(ValueError, match="SQL identifier"):
        wait_for_sql_fixture(
            "mirrored.sql.test",
            "TestMirroredDatabase",
            "secret-token",
            "fixture; DROP TABLE dbo.fixture",
        )
