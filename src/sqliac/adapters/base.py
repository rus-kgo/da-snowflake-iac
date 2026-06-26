"""Simplified, production-grade SQL adapter layer."""

from __future__ import annotations

import abc
import os
import tomllib
from pathlib import Path
from tomllib import TOMLDecodeError
from typing import Any, Type

import sqlparse

from sqliac import Paths
from sqliac.adapters.connection_manager import ThreadConnectionManager
from sqliac.errors import RustyError
from sqliac.template_engine import TemplateEngine


class BaseCredentials(abc.ABC):
    """Abstract base class establishing database connection credentials."""

    @property
    @abc.abstractmethod
    def dialect(self) -> str:
        """The identifier string for the target database system."""
        raise NotImplementedError(
            f"Credentials class {self.__class__.__name__} must implement 'dialect' property."
        )

    def __post_init__(self):
        """Validate if the required fields are present."""
        raise NotImplementedError(
            f"Credentials class {self.__class__.__name__} must implement post init validation."
        )


class BaseAdapter(abc.ABC):
    """Abstract base class handling configuration lifecycles and query execution."""

    # Define the concrete Connection Manager class
    ConnectionManager: Type[ThreadConnectionManager]

    # Standard Types
    Row = tuple[Any, ...] | None

    def __init__(self) -> None:
        # Load environment credentials immediately upon initialization
        self.credentials = self._load_credentials()
        self.connections = self.ConnectionManager(self.credentials)

    @property
    @abc.abstractmethod
    def dialect(self) -> str:
        """The identifier string for the target database system."""
        pass

    @property
    @abc.abstractmethod
    def credentials_class(self) -> type[BaseCredentials]:
        """The concrete Credentials class associated with this adapter."""
        raise NotImplementedError(
            f"Adapter class {self.__class__.__name__} must implement 'credentials_class' property."
        )

    @abc.abstractmethod
    def _fetch_row(self, cursor: Any) -> Row:
        """Process an open database cursor into standard formatted row."""
        raise NotImplementedError(
            f"Adapter class {self.__class__.__name__} must implement '_fetch_row' method."
        )

    def _load_credentials(self) -> BaseCredentials:
        """Load from file or environment variables prefixed with the dialect name."""
        target_path = self._resolve_path(Paths.PROVIDER_CREDENTIALS_FILE)
        if target_path.is_file():
            try:
                with open(target_path, "rb") as f:
                    config = tomllib.load(f)

                if self.dialect in config:
                    for key, value in config[self.dialect].items():
                        os.environ[f"{self.dialect.upper()}_{key.upper()}"] = str(value)

            except TOMLDecodeError as err:
                raise RustyError(
                    error="invalid TOML file",
                    file=str(target_path),
                    help="check TOML formatting in the definition file",
                ) from err

            except PermissionError:
                raise RustyError(
                    error="read permission denied",
                    file=str(target_path),
                    help="the file might be open by another application or missing read permissions",
                ) from None

        prefix = f"{self.dialect.upper()}_"
        credentials = {
            key.replace(prefix, "").lower(): val
            for key, val in os.environ.items()
            if key.startswith(prefix)
        }

        return self.credentials_class(**credentials)

    @staticmethod
    def _resolve_path(path: Path):
        return path if path.is_absolute() else Path.cwd() / path

    def _split_statements(self, sql: str) -> list[str]:
        """Cleans syntax comments and splits batches into single statements."""
        formatted = sqlparse.format(sql, strip_comments=True, strip_whitespace=True)
        return [str(s).strip() for s in sqlparse.parse(formatted) if str(s).strip()]

    def execute(self, sql: str) -> Row:
        """Executes one or more SQL statements sequentially within a clean connection lifecycle."""
        statements = self._split_statements(sql)

        with self.connections.get_connection() as conn:
            for statement in statements:
                try:
                    cursor = conn.cursor()
                    if cursor:
                        cursor.execute(sql)
                        return self._fetch_row(cursor)
                except Exception as err:
                    if isinstance(err, RustyError):
                        err.print()
                    raise RustyError(
                        error="SQL statement execution error",
                        sql=TemplateEngine.pretty_sql(sql=statement, as_syntax=False),
                        help=str(err),
                    ) from err

    def cleanup(self) -> None:
        """Cleans up all managed connections."""
        self.connections.cleanup_all()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup connections."""
        self.cleanup()
