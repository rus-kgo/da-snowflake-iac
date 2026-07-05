"""Snowflake adapter package for sqliac."""

__version__ = "0.1.0"

from .src.adapter import (
    SnowflakeAdapter,
    SnowflakeConnectionManager,
    SnowflakeCredentials,
)

__all__ = [
    "SnowflakeAdapter",
    "SnowflakeConnectionManager",
    "SnowflakeCredentials",
]
