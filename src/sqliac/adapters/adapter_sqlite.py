"""SQLite adapter implementation.

This module provides SQLite-specific implementations of the base adapter classes.
SQLite is simpler than Snowflake and useful for testing and development.
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Any, ClassVar
from sqliac.value_sanitizer import ValueSanitizer
from sqliac.errors import RustyError

from .base import (
    BaseAdapter,
    BaseConnectionManager,
    BaseCredentials,
    ConnectionState,
)


@dataclass
class SQLiteCredentials(BaseCredentials):
    """Credentials for SQLite connections.

    SQLite is file-based, so "credentials" are really just connection
    parameters like file path and pragmas.

    Attributes:
        database: Path to SQLite database file, or ':memory:' for in-memory
        schema: Not used for SQLite (always 'main')
        timeout: How long to wait for locks in seconds
        check_same_thread: Allow using connection from different threads
        isolation_level: Transaction isolation level (None for autocommit)
    """

    # Override database to have a default for SQLite
    database: str = ":memory:"
    schema: str = "main"
    timeout: int = 60

    @property
    def dialect(self) -> str:
        """Return adapter dialect identifier."""
        return "sqlite"

    def _connection_keys(self) -> tuple[str, ...]:
        """Return fields that uniquely identify a connection."""
        return ("database",)

    def connection_info(self) -> dict[str, Any]:
        """Return safe connection information for logging."""
        info = super().connection_info()
        info.update(
            {
                "timeout": self.timeout,
            }
        )
        return info


class SQLiteConnectionManager(BaseConnectionManager):
    """Connection manager for SQLite.

    Handles the specifics of connecting to SQLite databases,
    including file-based and in-memory databases.
    """

    DIALECT: ClassVar[str] = "sqlite"

    def __init__(self, credentials: SQLiteCredentials):
        """Initialize SQLite connection manager.

        Args:
            credentials: SQLiteCredentials object
        """
        super().__init__(credentials)
        self.credentials: SQLiteCredentials = credentials

    def _get_connect_args(self) -> dict[str, Any]:
        """Build connection arguments for SQLAlchemy.

        Returns:
            Dictionary of connection arguments
        """
        connect_args: dict[str, Any] = {
            "timeout": self.credentials.timeout,
        }

        return connect_args

    def open(self, connection: ConnectionState) -> ConnectionState:
        """Open a new SQLite connection.

        Args:
            connection: ConnectionState to populate

        Returns:
            ConnectionState with opened connection
        """
        try:
            # Get connection arguments
            conn_kwargs = self._get_connect_args()

            ctx = sqlite3.connect(database=self.credentials.database, **conn_kwargs)

            # Open connection
            handle = ctx.cursor()

            # Update connection state
            connection.state = "open"
            connection.handle = handle

        except Exception:
            # Mark connection as failed
            connection.state = "fail"
            raise
        else:
            return connection


class SQLiteAdapter(BaseAdapter):
    """Main adapter class for SQLite.

    This is the primary interface for working with SQLite databases
    in sqliac. It's particularly useful for:
    - Local development
    - Testing
    - Simple single-file databases

    Usage:
        # From configuration
        config = {...}  # From resources.toml
        credentials = SQLiteAdapter.get_credentials(config)

        # Create adapter
        with SQLiteAdapter(credentials) as adapter:
            adapter.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
    """

    DIALECT: ClassVar[str] = "sqlite"
    ConnectionManager: ClassVar[type] = SQLiteConnectionManager

    @classmethod
    def get_credentials(cls) -> SQLiteCredentials:
        """Parse configuration into SQLiteCredentials.

        This method extracts credentials from environment variables.

        Example env variables:

        env:
            DATABASE: <database name>
            TIMEOUT: <timeout in seconds>
        """
        # Extract from config with environment variable overrides
        sanitize = ValueSanitizer()

        database = os.environ.get("DATABASE", ":memory:")
        database = sanitize.to_string(database)

        timeout = os.environ.get("TIMEOUT", "30")
        timeout = sanitize.to_int(timeout, source="TIMEOUT")

        return SQLiteCredentials(
            database=database,
            schema="main",
            timeout=timeout,
        )

    def _fetchall(self, query_result: Any, result_format: str = "list") -> Any:
        """Fetch all results from SQLite query.

        Args:
            query_result: Cursor object from SQLite connection
            result_format: Result format ('list' for list of rows, 'dict' for list of dicts)

        Returns:
            Fetched result in the specified format:
            - 'list': List of tuples (default)
            - 'dict': List of dictionaries with column names as keys

        Example:
            ```
            with adapter.connections.get_connection() as conn:
                cursor = conn.execute("SELECT * FROM table")
                result = adapter.fetch(cursor, result_format="dict")
            ```
        """
        if query_result is None:
            return None

        try:
            # Fetch all rows from the cursor
            rows = query_result.fetchall()
        except AttributeError:
            raise RustyError(
                error="failed to fetch data from execution result.",
                details=f"{type(query_result).__name__}",
            ) from AttributeError
        else:
            if result_format == "dict":
                # Convert rows to dictionaries with column names
                # SQLite cursor.description contains column metadata
                if hasattr(query_result, "description") and query_result.description:
                    columns = [col[0] for col in query_result.description]
                    return [
                        {columns[i]: val for i, val in enumerate(row)} for row in rows
                    ]
                # Fallback: return as-is if no description
                return rows
            # Default: return list of tuples
            return rows
