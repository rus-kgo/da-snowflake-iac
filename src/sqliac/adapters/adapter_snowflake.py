from dataclasses import dataclass
from pathlib import Path
from typing import Any, Type

import snowflake.connector as sc
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.connector.cursor import SnowflakeCursor

from sqliac.adapters.base import BaseAdapter, BaseCredentials
from sqliac.adapters.connection_manager import ConnectionState, ThreadConnectionManager
from sqliac.constants import Paths
from sqliac.errors import RustyError


@dataclass(frozen=True)
class SnowflakeCredentials(BaseCredentials):
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
        return "snowflake"

    def __post_init__(self):
        if not all([self.account, self.user]):
            raise RustyError(error="Snowflake credentials must include account, user.")


class SnowflakeConnectionManager(ThreadConnectionManager):
    """Manages Snowflake connections with thread safety."""

    CredentialsClass = SnowflakeCredentials

    def open(self, connection: ConnectionState) -> ConnectionState:
        """Open a new Snowflake database connection."""
        try:
            conn_kwargs = self._build_connect_args()
            account = conn_kwargs.get("account", "unknown-account")
            print(f"info: '{account}' account connection thread: '{connection.name}'")
            conn = sc.connect(**conn_kwargs)
            connection.handle = conn
            connection.state = "open"
            return connection

        except Exception as e:
            print(f"failed to open connection for thread {connection.name}: {e}")
            connection.state = "fail"
            connection.handle = None
            raise RustyError(
                error="Failed to open Snowflake connection", help=str(e)
            ) from e

    def _build_connect_args(self) -> dict[str, Any]:
        """Build connection arguments for Snowflake."""
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

    def _get_private_key_bytes(self) -> bytes | None:
        """Process private key from path or string."""
        private_key_path = self.credentials.private_key_path
        private_key_str = self.credentials.private_key
        passphrase_str = self.credentials.private_key_passphrase

        if not private_key_path and not private_key_str:
            return None

        passphrase: bytes | None = None
        if passphrase_str:
            passphrase = passphrase_str.encode("utf-8")

        p_key = None
        if private_key_path:
            # Resolve path relative to project config directory
            path_obj = Paths.CONFIG_DIR / Paths.PROVIDER_DIR / private_key_path
            target_dir = self._resolve_path(path_obj)

            if not target_dir.exists() or not target_dir.is_file():
                raise RustyError(
                    error="not a valid `private_key_path`",
                    file=str(target_dir),
                    help=f"save the key file in the configuration directory: `{Paths.CONFIG_DIR / Paths.PROVIDER_DIR}`",
                )

            with open(target_dir, "rb") as key_file:
                p_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=passphrase,
                    backend=default_backend(),
                )
        elif private_key_str:
            private_key_clean = private_key_str.replace("\\n", "\n")
            p_key_bytes = private_key_clean.encode("utf-8")
            p_key = serialization.load_pem_private_key(
                p_key_bytes,
                password=passphrase,
                backend=default_backend(),
            )

        if p_key is None:
            return None

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @staticmethod
    def _resolve_path(path: Path):
        """Resolve path relative to current working directory."""
        return path if path.is_absolute() else Path.cwd() / path


class SnowflakeAdapter(BaseAdapter):
    """Snowflake implementation handling enterprise credential variables."""

    credentials: SnowflakeCredentials
    ConnectionManager = SnowflakeConnectionManager

    @property
    def dialect(self) -> str:
        return "snowflake"

    @property
    def credentials_class(self) -> Type[SnowflakeCredentials]:
        return SnowflakeCredentials

    def _fetch_row(self, cursor: SnowflakeCursor) -> BaseAdapter.Row:
        """Fetch row from the Snowflake cursor."""
        return cursor.fetchone()
