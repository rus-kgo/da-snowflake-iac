"""Base adapter classes for database connections.

This module implements the adapter pattern inspired by dbt-core's architecture.
It provides a standardized interface for connecting to different database systems
while allowing database-specific implementations.

Key Components:
- BaseCredentials: Type-safe credential configuration
- BaseConnectionManager: Manages connection lifecycle with thread safety
- BaseAdapter: High-level interface for database operations
"""

from __future__ import annotations

import abc
import threading
import sqlparse
import os
import tomllib
from pathlib import Path
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from sqliac.errors import RustyError
from sqliac.constants import Paths

if TYPE_CHECKING:
    from collections.abc import Iterator


type Row = dict[str, Any] | tuple[Any, ...]
type Rows = list[Row]


@dataclass
class BaseCredentials(abc.ABC):
    """Base credentials class for database connections.

    This class defines the interface for all database credentials.
    Each database adapter should subclass this and add its specific fields.

    Attributes:
        user: Username for authentification.
        host: The name of the host. Sometimes called as 'account'.
        warehouse (optional): The name of the compute resource.
    ---

    Example:
    ```
        @dataclass
        class SnowflakeCredentials(BaseCredentials):
            host: str
            user: str
            warehouse: str
            role:str
            password: Optionalstr] = None
            private_key_path: Optional[str] = None
            private_key: Option[str] = None
            ```
    """

    @property
    @abc.abstractmethod
    def dialect(self) -> str:
        """Return the adapter dialect identifier (e.g., 'snowflake', 'sqlite')."""
        raise NotImplementedError(
            f"Credentials class {self.__class__.__name__} must implement 'dialect' property"
        )

    @abc.abstractmethod
    def _connection_keys(self) -> tuple[str, ...]:
        """Return tuple of field names that uniquely identify a connection.

        This is used to determine if two credential sets represent the same
        logical connection. For example, for Snowflake this might be:
        ('account', 'user')
        """
        raise NotImplementedError(
            f"Credentials class {self.__class__.__name__} must implement '_connection_keys'"
        )

    def connection_info(self) -> dict[str, Any]:
        """Return a dictionary of connection information for logging.

        This method should NOT include sensitive information like passwords.
        """
        return {
            "dialect": self.dialect,
        }


@dataclass
class ConnectionState:
    """Represents the current state of a database connection.

    Attributes:
        name: Unique identifier for this connection thread
        state: Current state ('init','open', 'closed', 'fail')
        handle: The underlying database connection object
        credentials: Credentials used to establish this connection
    """

    name: str
    state: str = "init"
    handle: Any | None = None
    credentials: BaseCredentials | None = None


class BaseConnectionManager(abc.ABC):
    """Manages database connections with thread safety and connection pooling.

    This class is responsible for:
    - Opening and closing database connections
    - Managing a connection pool (one connection per thread)
    - Thread-safe access to connections
    - Connection lifecycle management

    The connection manager uses a threading.RLock to ensure thread safety
    when multiple threads access the connection pool simultaneously.

    Attributes:
        DIALECT: Class variable defining the adapter dialect
        thread_connections: Dictionary mapping thread names to connections
        lock: Reentrant lock for thread-safe operations
    """

    # Each adapter implementation must define its type
    DIALECT: ClassVar[str]

    def __init__(self, credentials: BaseCredentials):
        """Initialize the connection manager.

        Args:
            credentials: Database credentials for establishing connections
        """
        self.credentials = credentials

        # Dictionary to store one connection per thread
        # Key: thread name, Value: ConnectionState object
        self.thread_connections: dict[str, ConnectionState] = {}

        # Reentrant lock allows the same thread to acquire it multiple times
        # This is important because connection methods may call each other
        self.lock = threading.RLock()

    def get_thread_identifier(self) -> str:
        """Get unique identifier for the current thread.

        Returns:
            String identifier for the current thread (e.g., 'Thread-1')
        """
        return threading.current_thread().name

    def get_thread_connection(self) -> ConnectionState:
        """Get or create connection for the current thread.

        This method is thread-safe and will create a new connection
        if one doesn't exist for the current thread.

        Returns:
            ConnectionState object for the current thread
        """
        thread_id = self.get_thread_identifier()

        # Thread-safe access to the connections dictionary
        with self.lock:
            if thread_id not in self.thread_connections:
                # Create a new connection state for this thread
                self.thread_connections[thread_id] = ConnectionState(
                    name=thread_id,
                    credentials=self.credentials,
                )

        return self.thread_connections[thread_id]

    @abc.abstractmethod
    def open(self, connection: ConnectionState) -> ConnectionState:
        """Open a new database connection.

        This method must be implemented by each database adapter to handle
        the specific connection logic for that database.

        Args:
            connection: ConnectionState object to populate with connection

        Returns:
            Updated ConnectionState with opened connection

        Example (snowflake):
        ```
        try:
            # Get connection arguments
            conn_param = self._get_connect_args()

            # Create engine
            ctx = sc.connect(**conn_param)

            # Open connection
            handle = ctx.cursor()

            # Update connection state
            conn_state.state = "open"
            conn_state.handle = handle

            return conn_state

        except Exception:
            # Mark connection as failed
            conn_state.state = "fail"
            raise
        ```
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement the 'open' method"
        )

    def close(self, conn_state: ConnectionState) -> ConnectionState:
        """Close a database connection.

        Args:
            conn_state: ConnectionState with connection to close

        Returns:
            Updated ConnectionState with closed connection
        """
        if conn_state.state == "closed":
            # Connection already closed, nothing to do
            return conn_state

        if conn_state.handle is not None:
            try:
                # Handle custom connection types
                conn_state.handle.close()
            except Exception as e:
                print(f"warning: error closing connection {conn_state.name}: {e}")
                pass

        # Update state
        conn_state.state = "closed"
        conn_state.handle = None

        return conn_state

    def cleanup_all(self) -> None:
        """Close all open connections across all threads.

        This should be called when shutting down the application to ensure
        all database connections are properly closed.
        """
        with self.lock:
            for connection in self.thread_connections.values():
                if connection.state == "open":
                    self.close(connection)

            # Clear the connections dictionary
            self.thread_connections.clear()

    @contextmanager
    def get_connection(self) -> Iterator[Any]:
        """Context manager to get and manage a database connection.

        This ensures connections are properly opened and managed within
        the context. The connection is automatically associated with
        the current thread.

        Yields:
            SQL server connection object ready for use

        Example:
            ```
            with connection_manager.get_connection() as conn:
                result = conn.execute("SELECT 1")
            ```
        """
        # Get or create connection state for this thread
        conn_state = self.get_thread_connection()

        # Open connection if not already open
        if conn_state.state != "open":
            conn_state = self.open(conn_state)

        try:
            # Yield the actual database connection handle
            yield conn_state.handle  # pyright: ignore[reportReturnType]
        except Exception:
            # Mark connection as failed
            conn_state.state = "fail"
            raise


class BaseAdapter(abc.ABC):
    """High-level adapter interface for database operations.

    This class provides a high-level interface that combines the connection
    manager with additional database-specific functionality. It's the main
    entry point for interacting with databases in sqliac.

    Attributes:
        connections: The connection manager for this adapter
        DIALECT: The adapter type identifier
    """

    # Each adapter must define its type
    DIALECT: ClassVar[str]

    # Each adapter must specify its connection manager class
    ConnectionManager: ClassVar[type[BaseConnectionManager]]

    def __init__(self, credentials: BaseCredentials):
        """Initialize the adapter.

        Args:
            credentials: Database credentials
        """
        # Create the connection manager
        self.connections = self.ConnectionManager(credentials)

    @staticmethod
    def _resolve_path(path: str) -> Path:
        path_obj = Path(path)
        working_dir = Path.cwd()
        return path_obj if path_obj.is_absolute() else working_dir / path_obj

    @classmethod
    def load_credentials(cls) -> None:
        """Load credentials from file if available."""
        file_path = cls._resolve_path(
            f"{Paths.CONFIG_DIR}/{Paths.PROVIDER_DIR}/{Paths.CREDENTIALS_FILE}"
        )
        if file_path.is_file():
            with open(file_path, "rb") as f:
                config = tomllib.load(f)

            if cls.DIALECT in config:
                for key, value in config[cls.DIALECT].items():
                    os.environ[f"{cls.DIALECT.upper()}_{key.upper()}"] = str(value)

    @classmethod
    @abc.abstractmethod
    def get_credentials(cls) -> BaseCredentials:
        """Parse configuration dictionary into credentials object.

        This method should be overridden by each adapter to handle
        its specific credential requirements.

        Returns:
            Credentials object for this adapter
        """
        raise NotImplementedError(
            f"{cls.__name__} must implement 'get_credentials' class method"
        )

    def _split_sql_statements(self, sql: str) -> list[str]:
        """Split multi-statement SQL into individual statements.

        Uses sqlparse with proper handling of strings and comments.
        """
        # Remove comments first
        sql_no_comments = sqlparse.format(
            sql=sql, strip_comments=True, strip_whitespace=True
        )

        # Parse into statements (sqlparse handles strings correctly)
        parsed = sqlparse.parse(sql_no_comments)

        # Filter out empty statements
        return [str(stmt).strip() for stmt in parsed if str(stmt).strip()]

    def execute(self, sql: str) -> Rows:
        """Execute SQL with the connection manager.

        Args:
            sql (str): SQL statement(s) to execute (can be multiple statements)

        """
        result = []

        with self.connections.get_connection() as conn:
            # Split SQL into individual statements
            statements = self._split_sql_statements(sql)

            for statement in statements:
                try:
                    # Only the last cursor is kept
                    cursor = conn.execute(statement)
                    if cursor:
                        result = self._fetchall(cursor)
                except Exception as err:
                    raise RustyError(
                        error="SQL excecution failed.",
                    ) from err

        return result

    @abc.abstractmethod
    def _fetchall(self, cursor: Any) -> Rows:
        """Template method for adapters to override.

        Must return a list of rows (dictionaries or tuples).

        Args:
            cursor: Database cursor from execute()
        """
        return cursor.fetchall()

    def cleanup(self) -> None:
        """Clean up all connections."""
        self.connections.cleanup_all()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup connections."""
        self.cleanup()
