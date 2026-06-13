"""Adapter factory for creating database adapters.

This module provides a factory pattern for creating the appropriate
adapter based on the database system type. It automatically discovers
available adapters and instantiates the correct one.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqliac.adapters.base import BaseAdapter

from sqliac.adapters.adapter_snowflake import SnowflakeAdapter
from sqliac.adapters.adapter_sqlite import SQLiteAdapter
from sqliac.errors import RustyError


class AdapterFactory:
    """Factory for creating database adapters.

    This class maintains a registry of available adapters and creates
    the appropriate adapter instance based on the database system type.

    The factory pattern allows easy extension with new adapters without
    modifying existing code.
    """

    # Registry of available adapters
    # Key: adapter type (e.g., 'snowflake', 'sqlite')
    # Value: adapter class
    _ADAPTERS: dict[str, type[BaseAdapter]] = {
        "snowflake": SnowflakeAdapter,
        "sqlite": SQLiteAdapter,
    }

    @classmethod
    def register_adapter(
        cls,
        provider: str,
        adapter_class: type[BaseAdapter],
    ) -> None:
        """Register a new adapter type.

        This allows for dynamic registration of custom adapters.

        Args:
            provider: Type identifier (e.g., 'postgresql', 'oracle')
            adapter_class: Adapter class to register

        Example:
            class OracleAdapter(BaseAdapter):
                TYPE = "oracle"
                ...

            AdapterFactory.register_adapter("oracle", OracleAdapter)
        """
        if provider in cls._ADAPTERS:
            raise RustyError(error=f"`{provider}` adapter is already registered")

        cls._ADAPTERS[provider] = adapter_class

    @classmethod
    def get_adapter(
        cls,
        provider: str,
    ) -> BaseAdapter:
        """Create and return an adapter instance.

        This method:
        1. Looks up the adapter class for the database system
        2. Parses credentials from the configuration
        3. Creates and returns an adapter instance

        Args:
            provider: Type of database ('snowflake', 'sqlite', etc.)
            config: Configuration dictionary from resources.toml

        Returns:
            Initialized adapter instance ready for use

        Raises:
            ValueError: If provider is not supported

        Example:
            config = {
                'sqlalchemy.url': 'snowflake://',
                'sqlalchemy.connect_args': {...}
            }

            adapter = AdapterFactory.get_adapter('snowflake', config)
            with adapter:
                adapter.execute('SELECT 1')
        """
        # Normalize database system name to lowercase
        db_system = provider.lower()

        # Check if adapter exists
        if db_system not in cls._ADAPTERS:
            available = ", ".join(cls._ADAPTERS.keys())
            raise RustyError(
                error=f"unsupported provider: `{provider}`. ",
                details=f"available adapters: {available}",
            )

        # Get adapter class
        adapter_class = cls._ADAPTERS[db_system]

        adapter_class.load_credentials()

        # Parse credentials from config
        credentials = adapter_class.get_credentials()

        # Create and return adapter instance
        return adapter_class(credentials)

    @classmethod
    def list_adapters(cls) -> list[str]:
        """Return list of all registered adapter types.

        Returns:
            List of adapter type identifiers

        Example:
            >>> AdapterFactory.list_adapters()
            ['snowflake', 'sqlite']
        """
        return list(cls._ADAPTERS.keys())
