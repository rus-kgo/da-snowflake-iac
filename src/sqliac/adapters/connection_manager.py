"""Manages database connections with thread safety and connection pooling."""

import abc
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, ClassVar, Generator


@dataclass
class ConnectionState:
    """Represents the current state of a database connection.

    Args:
        name(str): Unique identifier for this connection thread
        state(str): Current state ('init', 'open', 'closed', 'fail')
        handle(str): The underlying database connection object
        credentials(str): Credentials used to establish this connection
    """

    name: str
    state: str = "init"
    handle: Any | None = None
    credentials: Any | None = None


class ThreadConnectionManager(abc.ABC):
    """Manages database connections with thread safety."""

    # Each adapter implementation must define its type of credentials
    CredentialsClass: ClassVar[type[Any]]

    def __init__(self, credentials: Any):
        """Initialize the connection manager.

        Args:
            credentials: Database credentials for establishing connections
        """
        self.credentials = credentials

        # Dictionary to store one connection per thread
        # Key: thread name, Value: ConnectionState object
        self.thread_connections: dict[str, ConnectionState] = {}

        # Reentrant lock allows the same thread to acquire it multiple times
        self.lock = threading.RLock()

    def get_thread_identifier(self) -> str:
        """Get unique identifier for the current thread."""
        return threading.current_thread().name

    def get_thread_connection(self) -> ConnectionState:
        """Get or create connection for the current thread."""
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
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement the 'open' method"
        )

    def close(self, conn_state: ConnectionState) -> ConnectionState:
        """Close a database connection."""
        if conn_state.state == "closed":
            return conn_state

        if conn_state.handle is not None:
            try:
                # Handle custom connection types
                conn_state.handle.close()
            except Exception as e:
                # Log warning instead of raising, as we are cleaning up
                print(f"warning: error closing connection {conn_state.name}: {e}")
                pass

        # Update state
        conn_state.state = "closed"
        conn_state.handle = None

        return conn_state

    def cleanup_all(self) -> None:
        """Close all open connections across all threads."""
        with self.lock:
            for connection in self.thread_connections.values():
                if connection.state == "open":
                    self.close(connection)
            # Clear the connections dictionary
            self.thread_connections.clear()

    @contextmanager
    def get_connection(self) -> Generator[Any]:
        """Context manager to get and manage a database connection.

        Yields:
            SQL server connection object ready for use
        """
        # Get or create connection state for this thread
        conn_state = self.get_thread_connection()

        # Open connection if not already open
        if conn_state.state != "open":
            # Ensure the concrete open method is called
            conn_state = self.open(conn_state)

        try:
            # Yield the actual database connection handle
            yield conn_state.handle  # pyright: ignore[reportReturnType]
        except Exception:
            # Mark connection as failed on exception
            conn_state.state = "fail"
            raise
