# src/sqliac/provider_loader.py
"""Loads and validates a provider directory into a typed structure."""

from __future__ import annotations

import tomllib
from dataclasses import InitVar, asdict, dataclass, field
from importlib.resources import files
from pathlib import Path
from tomllib import TOMLDecodeError
from typing import TYPE_CHECKING, Any

from sqliac.adapters import AdapterFactory
from sqliac.constants import DDLCommand, Paths
from sqliac.errors import RustyError

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable


@dataclass(frozen=True)
class ResourceConfig:
    """Contents of resources configuration.

    Args:
        ddl_template(str): Contents of template.sql
        state_query(str): Contents of state.sql
        ddl_command(dict):DDL command create/alter/drop
        ddl_context(dict):DDL Jinja template context
    """

    ddl_template: str = ""
    state_query: str = ""
    ddl_command: dict = field(default_factory=dict)
    ddl_context: dict = field(default_factory=dict)

    config_file_path: InitVar[str] = ""
    resource_type: InitVar[str] = ""

    def __post_init__(self, config_file_path: str, resource_type: str):
        """Standard library validation hook."""
        if not isinstance(self.ddl_command, dict):
            raise RustyError(error="`ddl_command` must be a dictionary")
        if not isinstance(self.ddl_context, dict):
            raise RustyError(error="`ddl_context` must be a dictionary")

        if "name" not in self.ddl_context:
            raise RustyError(
                error=f"`{resource_type}` is missing `name` field.",
                file=config_file_path,
            )
        if "depends_on" not in self.ddl_context:
            raise RustyError(
                error=f"`{resource_type}` is missing `depends_on` field.",
                file=config_file_path,
                help=f"add `[{resource_type}.ddl_context.depends_on]` to config (can be empty)",
            )

        valid_enum_values = set(DDLCommand.executable())

        for key in self.ddl_command:
            if key not in valid_enum_values:
                raise RustyError(
                    error=f"invalid DDL command `{key}` in the config. ",
                    help=f"must be one of: {', '.join(valid_enum_values)}",
                )

    def to_dict(self):
        """Return as dictionary."""
        return asdict(self)


@dataclass(frozen=True)
class ProviderConfig:
    """Contents of provider configuration.

    Args:
        name(str): Provider name
        resources(dict[str, ResourceConfig]): Provider resources configuration
    """

    name: str
    resources: dict[str, ResourceConfig]

    def to_dict(self):
        """Return as dictionary."""
        return asdict(self)


def _resolve_path(path: Path):
    return path if path.is_absolute() else Path.cwd() / path


class ProviderLoader:
    """Loads a provider directory into a ProviderConfig."""

    @classmethod
    def load(cls, single_resource: str | None = None):
        """Load Provider configuration."""
        provider_name, resources_raw = cls._load_config()

        if single_resource:
            if single_resource not in resources_raw:
                raise RustyError(
                    error=f"resource '{single_resource}' not found in config.",
                    file=str(Paths.PROVIDER_CONFIG_FILE),
                )
            items_to_process = {single_resource: resources_raw[single_resource]}
        else:
            items_to_process = resources_raw

        resources = {
            resource_type: cls._parse_single_resource(resource_type, config)
            for resource_type, config in items_to_process.items()
        }
        return ProviderConfig(name=provider_name, resources=resources)

    @classmethod
    def _load_config(cls) -> tuple:
        target_dir = (
            Paths.PROVIDER_CONFIG_FILE
            if Paths.PROVIDER_CONFIG_FILE.is_absolute()
            else Path.cwd() / Paths.PROVIDER_CONFIG_FILE
        )

        if not target_dir.is_file():
            raise RustyError(
                error="missing `config.toml`",
                file=str(target_dir),
                help="run `sqliac init` to create scaffolding with required files",
            )

        try:
            with open(target_dir, "rb") as f:
                config_data = tomllib.load(f)
        except TOMLDecodeError as err:
            raise RustyError(
                error="invalid TOML file",
                file=str(target_dir),
                help="check TOML formatting in the definition file",
            ) from err

        except PermissionError:
            raise RustyError(
                error="read permission denied",
                file=str(target_dir),
                help="the file might be open by another application or missing read permissions",
            ) from None

        else:
            provider_name = next(iter(config_data))
            available_adapters = AdapterFactory().list_adapters()
            if provider_name not in available_adapters:
                raise RustyError(
                    error=f"`{provider_name}` adapter not available",
                    details=f"""available adapters:
    -  {"\n -  ".join(available_adapters)}
    """,
                )

            resources_raw = config_data.get(provider_name)
            if not resources_raw:
                raise RustyError(
                    error=f"`{provider_name}` config contains no resources",
                    file=str(target_dir),
                    help="""`config.toml` example below
    ```toml
    [snowflake.table.ddl_command]
    create = "CREATE OR ALTER"
    alter  = "CREATE OR ALTER"
    drop   = "DROP"

    [snowflake.table.ddl_context]
    name = "example_table"

    [snowflake.table.ddl_context.depends_on]
    database = ["expl_db_one"]
    schema = ["exmpl_sch_one"]
    ```
    """,
                )

            return provider_name, resources_raw

    @classmethod
    def _parse_single_resource(
        cls, resource_type: str, config: dict[str, Any]
    ) -> ResourceConfig:
        if not Paths.ddl_template_file(resource_type).is_file():
            raise RustyError(
                error=f"missing `{resource_type}` DDL template.",
                file=str(Paths.ddl_template_file(resource_type)),
            )
        if not Paths.state_file(resource_type).is_file():
            raise RustyError(
                error=f"missing `{resource_type}` State template.",
                file=str(Paths.state_file(resource_type)),
            )

        config_file_path = (
            Paths.PROVIDER_CONFIG_FILE
            if Paths.PROVIDER_CONFIG_FILE.is_absolute()
            else Path.cwd() / Paths.PROVIDER_CONFIG_FILE
        )
        return ResourceConfig(
            ddl_template=cls._read_sql(Paths.ddl_template_file(resource_type)),
            state_query=cls._read_sql(Paths.state_file(resource_type)),
            ddl_command=config.get("ddl_command", {}),
            ddl_context=config.get("ddl_context", {}),
            config_file_path=str(config_file_path),
            resource_type=resource_type,
        )

    @staticmethod
    def _read_sql(path: Path) -> str:
        with open(path) as f:
            return f.read()

    @classmethod
    def _copy_resources(cls, source_dir: Traversable, target_dir: Path):
        for item in source_dir.iterdir():
            dest = target_dir / item.name
            if item.is_file():
                dest.write_text(item.read_text())
            else:
                dest.mkdir(exist_ok=True)
                cls._copy_resources(item, dest)

    @classmethod
    def init_providers(cls):
        """Create providers scaffolding."""
        source_dir = files(Paths.TEMPLATES_ANCHOR) / Paths.PROVIDER_DIR

        target_dir = _resolve_path(Paths.CONFIG_DIR / Paths.PROVIDER_DIR)

        target_dir.mkdir(parents=True, exist_ok=True)

        cls._copy_resources(source_dir, target_dir)
