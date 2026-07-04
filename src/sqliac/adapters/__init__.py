"""Adapters package for sqliac."""

__version__ = "0.1.0"

from .base import (
    BaseAdapter,
    BaseCredentials,
)
from .factory import AdapterFactory

__all__ = [
    # Base classes
    "BaseAdapter",
    "BaseCredentials",
    # Factory
    "AdapterFactory",
]
