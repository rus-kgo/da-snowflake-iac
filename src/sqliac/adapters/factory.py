"""Adapter factory for creating database adapters."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqliac.adapters.base import BaseAdapter

from sqliac.adapters.adapter_snowflake import SnowflakeAdapter
from sqliac.errors import RustyError


class AdapterFactory:
    """Factory for creating database adapters"""

    # Registry of available adapters
    # Key: adapter type (e.g., 'snowflake', 'sqlite')
    # Value: adapter class
    _ADAPTERS: dict[str, type[BaseAdapter]] = {
        "snowflake": SnowflakeAdapter,
    }

    @classmethod
    def register_adapter(
        cls,
        provider: str,
        adapter_class: type[BaseAdapter],
    ) -> None:
        """Register a new adapter type."""
        if provider in cls._ADAPTERS:
            raise RustyError(error=f"`{provider}` adapter is already registered")

        cls._ADAPTERS[provider] = adapter_class

    @classmethod
    def get_adapter(
        cls,
        provider: str,
    ) -> BaseAdapter:
        """Create and return an adapter instance."""
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

        # Create and return adapter instance
        return adapter_class()

    @classmethod
    def list_adapters(cls) -> list[str]:
        """Return list of all registered adapter types."""
        return list(cls._ADAPTERS.keys())
