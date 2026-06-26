"""Simplified, production-grade SQL adapter layer."""

from __future__ import annotations

import abc
import os
from contextlib import contextmanager
from typing import Any, Generator

import sqlparse

# Standard Types
type Row = dict[str, Any] | tuple[Any, ...]
type Rows = list[Row]


class BaseAdapter(abc.ABC):
    """Abstract base class handling configuration lifecycles and query execution."""

    def __init__(self) -> None:
        # Load environment credentials immediately upon initialization
        self.config = self._load_env_credentials()

    @property
    @abc.abstractmethod
    def dialect(self) -> str:
        """The identifier string for the target database system."""
        pass

    @abc.abstractmethod
    @contextmanager
    def _connection(self) -> Generator[Any, None, None]:
        """Context manager yielding an active database connection or cursor."""
        pass

    @abc.abstractmethod
    def _fetch_rows(self, cursor: Any) -> Rows:
        """Process an open database cursor into standard formatted rows."""
        pass

    def _load_env_credentials(self) -> dict[str, str]:
        """Scans environment variables prefixed with the dialect name."""
        prefix = f"{self.dialect.upper()}_"
        return {
            key.replace(prefix, "").lower(): val
            for key, val in os.environ.items()
            if key.startswith(prefix)
        }

    def _split_statements(self, sql: str) -> list[str]:
        """Cleans syntax comments and splits batches into single statements."""
        formatted = sqlparse.format(sql, strip_comments=True, strip_whitespace=True)
        return [str(s).strip() for s in sqlparse.parse(formatted) if str(s).strip()]

    def execute(self, sql: str) -> Rows:
        """Executes one or more SQL statements sequentially within a clean connection lifecycle."""
        statements = self._split_statements(sql)
        results: Rows = []

        # The context manager automatically guarantees cleanup even if queries crash
        with self._connection() as conn:
            for statement in statements:
                try:
                    # In real environments, you would use conn.cursor()
                    cursor = conn.execute(statement)
                    if cursor:
                        results = self._fetch_rows(cursor)
                except Exception as err:
                    raise RuntimeError(
                        f"Database execution error on system '{self.dialect}' during statement:\n"
                        f"  👉 {statement}\n"
                        f"Details: {err}"
                    ) from err

        return results


# =====================================================================
# CONCRETE IMPLEMENTATIONS (Completely self-contained and transparent)
# =====================================================================


class FakeSQLiteCursor:
    """Mock SQLite cursor to simulate native database responses."""

    def execute(self, statement: str) -> FakeSQLiteCursor:
        print(f"[SQLite Cursor] Executing: {statement}")
        return self

    def fetchall(self) -> list[tuple]:
        return [(1, "Alice"), (2, "Bob")]


class SQLiteAdapter(BaseAdapter):
    """SQLite implementation requiring no explicit background threading blocks."""

    @property
    def dialect(self) -> str:
        return "sqlite"

    @contextmanager
    def _connection(self) -> Generator[Any, None, None]:
        # Mock Connection Engine Setup
        db_path = self.config.get("path", ":memory:")
        print(f"\n[SQLite Engine] Connecting to database at -> {db_path}")

        # In real code: conn = sqlite3.connect(db_path)
        conn = FakeSQLiteCursor()
        try:
            yield conn
        finally:
            print("[SQLite Engine] Disconnecting and closing cleanly.")

    def _fetch_rows(self, cursor: Any) -> Rows:
        # Convert raw SQLite tuples into standard descriptive dicts
        return [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]


class FakeSnowflakeCursor:
    """Mock Snowflake cursor to simulate cloud database responses."""

    def execute(self, statement: str) -> FakeSnowflakeCursor:
        print(f"[Snowflake Cursor] Dispatching query to warehouse: {statement}")
        return self

    def fetchall(self) -> list[dict]:
        return [{"USER_ID": 99, "STATUS": "ACTIVE"}]


class SnowflakeAdapter(BaseAdapter):
    """Snowflake implementation handling enterprise credential variables."""

    @property
    def dialect(self) -> str:
        return "snowflake"

    @contextmanager
    def _connection(self) -> Generator[Any, None, None]:
        account = self.config.get("account", "unknown-account")
        print(f"\n[Snowflake Engine] Initializing cloud session for account: {account}")

        # In real code: conn = snowflake.connector.connect(...)
        conn = FakeSnowflakeCursor()
        try:
            yield conn
        finally:
            print("[Snowflake Engine] Terminating cloud network connection cleanly.")

    def _fetch_rows(self, cursor: Any) -> Rows:
        return cursor.fetchall()


# =====================================================================
# EXECUTION SIMULATION
# =====================================================================

if __name__ == "__main__":
    # 1. Inject some mock environment variables (simulating your TOML engine/OS vars)
    os.environ["SQLITE_PATH"] = "/var/data/local.db"
    os.environ["SNOWFLAKE_ACCOUNT"] = "xyz12345.us-east-1"

    # 2. Instantiate and run the SQLite Adapter directly
    sqlite_client = SQLiteAdapter()
    sql_batch = "-- Initial setup \n CREATE TABLE users (id INT); SELECT * FROM users;"

    sqlite_data = sqlite_client.execute(sql_batch)
    print(f"Resulting Output: {sqlite_data}")

    # 3. Instantiate and run the Snowflake Adapter directly
    snowflake_client = SnowflakeAdapter()
    snowflake_data = snowflake_client.execute("SELECT * FROM raw.users;")
    print(f"Resulting Output: {snowflake_data}")
