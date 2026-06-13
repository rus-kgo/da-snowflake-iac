"""Adapters package for sqliac."""

__version__ = "0.1.0"

from .base import (
    BaseAdapter,
    BaseConnectionManager,
    BaseCredentials,
    ConnectionState,
)
from .factory import AdapterFactory
from .adapter_snowflake import (
    SnowflakeAdapter,
    SnowflakeConnectionManager,
    SnowflakeCredentials,
)
from .adapter_sqlite import (
    SQLiteAdapter,
    SQLiteConnectionManager,
    SQLiteCredentials,
)

__all__ = [
    # Base classes
    "BaseAdapter",
    "BaseConnectionManager",
    "BaseCredentials",
    "ConnectionState",
    # Factory
    "AdapterFactory",
    # Snowflake
    "SnowflakeAdapter",
    "SnowflakeConnectionManager",
    "SnowflakeCredentials",
    # SQLite
    "SQLiteAdapter",
    "SQLiteConnectionManager",
    "SQLiteCredentials",
]
