"""Snowflake adapter implementation.

This module provides Snowflake-specific implementations of the base adapter classes.
It handles Snowflake's unique authentication methods including password, key-pair,
and OAuth authentication.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, ClassVar
import re
import snowflake.connector as sc
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pathlib import Path

from sqliac.errors import RustyError
from sqliac.constants import Paths
from .base import (
    BaseAdapter,
    BaseConnectionManager,
    BaseCredentials,
    ConnectionState,
)


def to_str(s: str | None) -> str:
    """Make sure it a string, return None empty."""
    if s is None or s in {"None", ""}:
        return ""
    return s


@dataclass
class SnowflakeCredentials(BaseCredentials):
    """Credentials for Snowflake connections.

    Supports multiple authentication methods:
    1. Username + Password
    2. Username + Private Key (key-pair authentication)
    3. OAuth (client credentials flow)

    Attributes:
        account: Snowflake account identifier (e.g., 'abc12345.us-east-1')
        user: Username for authentication
        role: Role to assume
        warehouse: Virtual warehouse to use
        password (optional): Password for basic auth
        database (optional): Target database
        schema (optional): Target schema
        private_key_path (optional): Path to private key file for key-pair auth
        private_key (optional): PEM-encoded private key string
        private_key_passphrase (optional): Passphrase for encrypted private key
    """

    # TODO: add browser auth
    account: str
    user: str
    warehouse: str | None = None
    database: str | None = None
    schema: str | None = None
    role: str | None = None
    password: str | None = None
    private_key_path: str | None = None
    private_key: str | None = None
    private_key_passphrase: str | None = None

    @property
    def dialect(self) -> str:
        """Return adapter dialect identifier."""
        return "snowflake"

    def _connection_keys(self) -> tuple[str, ...]:
        """Return fields that uniquely identify a connection."""
        return (
            "account",
            "user",
            "warehouse",
            "role",
        )

    def connection_info(self) -> dict[str, Any]:
        """Return safe connection information for logging."""
        info = super().connection_info()
        info.update(
            {
                "account": self.account,
                "user": self.user,
                "warehouse": self.warehouse,
                "role": self.role,
            }
        )
        return info


class SnowflakeConnectionManager(BaseConnectionManager):
    """Connection manager for Snowflake.

    Handles the specifics of connecting to Snowflake, including:
    - Multiple authentication methods
    - Private key processing
    - SQLAlchemy engine configuration
    """

    DIALECT: ClassVar[str] = "snowflake"

    def __init__(self, credentials: SnowflakeCredentials):
        """Initialize Snowflake connection manager.

        Args:
            credentials: SnowflakeCredentials object
        """
        super().__init__(credentials)
        self.credentials: SnowflakeCredentials = credentials

    @staticmethod
    def _resolve_private_key_path(private_key_path: str) -> Path:
        path_obj = Path(f"{Paths.CONFIG_DIR}\\{Paths.PROVIDER_DIR}\\{private_key_path}")
        working_dir = Path.cwd()
        target_dir = path_obj if path_obj.is_absolute() else working_dir / path_obj

        if not target_dir.exists() or not target_dir.is_file():
            raise RustyError(
                error="not a valid `private_key_path`",
                file=str(target_dir),
                help=f"save the key file the configuration directory: `{Paths.CONFIG_DIR}\\{Paths.PROVIDER_DIR}\\`",
            )
        return target_dir

    def _get_private_key_bytes(self) -> bytes | None:
        """Process private key from path or string.

        This handles loading and deserializing the private key for
        Snowflake's key-pair authentication.

        Returns:
            Private key in DER-encoded PKCS8 format, or None if no key provided
        """
        private_key_path = self.credentials.private_key_path
        private_key_str = self.credentials.private_key
        passphrase_str = self.credentials.private_key_passphrase

        # No private key provided
        if not private_key_path and not private_key_str:
            return None

        # Convert passphrase to bytes if provided
        passphrase: bytes | None = None
        if passphrase_str:
            passphrase = passphrase_str.encode("utf-8")

        # Load private key
        p_key = None

        if private_key_path:
            target_path = self._resolve_private_key_path(private_key_path)

            with open(target_path, "rb") as key_file:
                p_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=passphrase,
                    backend=default_backend(),
                )
        elif private_key_str:
            # Load from string
            # Handle escaped newlines (common in environment variables)
            private_key_clean = private_key_str.replace("\\n", "\n")
            p_key_bytes = private_key_clean.encode("utf-8")

            p_key = serialization.load_pem_private_key(
                p_key_bytes,
                password=passphrase,
                backend=default_backend(),
            )

        if p_key is None:
            return None

        # Convert to DER-encoded PKCS8 format (required by Snowflake connector)
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    def _get_connect_args(self) -> dict[str, Any]:
        """Build connection arguments for Snowflake.

        Returns:
            Dictionary of connection arguments
        """
        connect_args: dict[str, Any] = {
            "account": self.credentials.account,
            "user": self.credentials.user,
        }

        # Add optional parameters if provided
        if self.credentials.database:
            connect_args["database"] = self.credentials.database

        if self.credentials.schema:
            connect_args["schema"] = self.credentials.schema

        if self.credentials.warehouse:
            connect_args["warehouse"] = self.credentials.warehouse

        if self.credentials.role:
            connect_args["role"] = self.credentials.role

        # Authentication: prioritize private key over password
        private_key_bytes = self._get_private_key_bytes()
        if private_key_bytes:
            connect_args["private_key"] = private_key_bytes
            connect_args["authenticator"] = "SNOWFLAKE_JWT"
        elif self.credentials.password:
            connect_args["password"] = self.credentials.password
        else:
            raise RustyError(
                error="credentials error",
                help="either password or private_key must be provided for authentication",
            )

        return connect_args

    def open(self, conn_state: ConnectionState) -> ConnectionState:
        """Open a new Snowflake connection.

        Args:
            conn_state: ConnectionState to populate

        Returns:
            ConnectionState with opened connection
        """
        try:
            conn_kwargs = self._get_connect_args()
            # Create connection
            ctx = sc.connect(**conn_kwargs)

            # Open connection
            handle = ctx.cursor()

            # Update connection state
            conn_state.state = "open"
            conn_state.handle = handle

            return conn_state  # noqa: TRY300

        except Exception:
            # Mark connection as failed
            conn_state.state = "fail"
            raise


class SnowflakeAdapter(BaseAdapter):
    """Main adapter class for Snowflake.

    This is the primary interface for working with Snowflake databases
    in sqliac. It combines the connection manager with Snowflake-specific
    functionality.

    Usage:
        # From configuration
        config = {...}  # From resources.toml
        credentials = SnowflakeAdapter.get_credentials(config)

        # Create adapter
        with SnowflakeAdapter(credentials) as adapter:
            adapter.execute("CREATE TABLE test (id INT)")
    """

    DIALECT: ClassVar[str] = "snowflake"
    ConnectionManager: ClassVar[type] = SnowflakeConnectionManager

    @classmethod
    def get_credentials(cls) -> SnowflakeCredentials:
        """Parse configuration into SnowflakeCredentials.

        This method extracts credentials from environment variables.

        Returns:
            SnowflakeCredentials object

        Example env variables:

        env:
            USER: <account user name>
            ACCOUNT: <also called as 'host'>
            WAREHOUSE: <warehouse name>
        """
        # Build credentials from environment
        # Required fields with validation
        account = to_str(os.environ.get("SNOWFLAKE_ACCOUNT"))
        if not account:
            raise RustyError(
                error="`SNOWFLAKE_ACCOUNT` environment variable is required"
            )

        user = to_str(os.environ.get("SNOWFLAKE_USER"))
        if not user:
            raise RustyError(error="`SNOWFLAKE_USER` environment variable is required")

        # Validate host format (basic check)
        if not re.match(r"^[a-zA-Z0-9_-]+$", account):
            raise RustyError(error=f"invalid `SNOWFLAKE_ACCOUNT` format: `{account}`")

        warehouse = to_str(os.environ.get("SNOWFLAKE_WAREHOUSE"))
        role = to_str(os.environ.get("SNOWFLAKE_ROLE"))

        # Optional fields
        password = to_str(os.environ.get("SNOWFLAKE_PASSWORD"))
        private_key_path = to_str(os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"))
        private_key = to_str(os.environ.get("SNOWFLAKE_PRIVATE_KEY"))
        private_key_passphrase = to_str(
            os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        )

        return SnowflakeCredentials(
            account=account,
            user=user,
            password=password,
            warehouse=warehouse,
            role=role,
            private_key_path=private_key_path,
            private_key=private_key,
            private_key_passphrase=private_key_passphrase,
        )

    def _fetchall(self, cursor: Any) -> list[dict]:
        """Fetch all results from Snowflake query."""
        rows = cursor.fetchall()
        if not rows or not cursor.description:
            return rows

        columns = [col[0] for col in cursor.description]
        return [{columns[i]: val for i, val in enumerate(row)} for row in rows]
