# src/sqliac/provider_loader.py
"""Loads and validates a provider directory into a typed structure."""

from __future__ import annotations

import tomllib
from importlib.resources import files
from dataclasses import dataclass, asdict, field, InitVar
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqliac.errors import RustyError
from sqliac.adapters import AdapterFactory
from sqliac.constants import DDLCommand, Paths

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
    resource_name: InitVar[str] = ""

    def __post_init__(self, config_file_path: str, resource_name: str):
        """Standard library validation hook."""
        if not isinstance(self.ddl_command, dict):
            raise RustyError(error="`ddl_command` must be a dictionary")
        if not isinstance(self.ddl_context, dict):
            raise RustyError(error="`ddl_context` must be a dictionary")

        if "name" not in self.ddl_context:
            raise RustyError(
                error=f"`{resource_name}` is missing `name` field.",
                file=config_file_path,
            )
        if "depends_on" not in self.ddl_context:
            raise RustyError(
                error=f"`{resource_name}` is missing `depends_on` field.",
                file=config_file_path,
                help=f"add `[{resource_name}.ddl_context.depends_on]` to config (can be empty)",
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


class ProviderLoader:
    """Loads a provider directory into a ProviderConfig."""

    def __init__(self, directory: str) -> None:  # noqa: D107
        self.directory_path = self._validate_dir(directory)

    def load(self):
        """Load Provider configuration."""
        provider_name, resources_raw = self._load_config()
        resources = self._parse_resources(resources_raw)
        return ProviderConfig(name=provider_name, resources=resources)

    @staticmethod
    def _validate_dir(directory: str) -> Path:
        directory_path = Path(directory)
        if not directory_path.exists() or not directory_path.is_dir():
            raise RustyError(
                error="provider folder not found",
                file=f".\\{directory}",
                help="the root of your project must contain provider configuration in the `provider` folder",
            )
        return directory_path

    def _load_config(self) -> tuple:
        config_path = self.directory_path / Paths.CONFIG_FILE
        if not config_path.exists():
            raise RustyError(
                error="missing `config.toml` in provider folder",
                file=str(config_path),
                help="""create `config.toml` with resource metadata, example below
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

        with open(config_path, "rb") as f:
            config_data = tomllib.load(f)

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
                help="""create `config.toml` with resource metadata, example below
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

    def _parse_resources(
        self, resources_raw: dict[str, dict[str, Any]]
    ) -> dict[str, ResourceConfig]:
        resources = {}

        for resource_name, config in resources_raw.items():
            resource_dir = self.directory_path / resource_name

            self._validate_resource_dir(resource_name, resource_dir)

            resources[resource_name] = ResourceConfig(
                ddl_template=self._read_sql(resource_dir / Paths.DDL_TEMPLATE_FILE),
                state_query=self._read_sql(resource_dir / Paths.STATE_FILE),
                ddl_command=config.get("ddl_command", {}),
                ddl_context=config.get("ddl_context", {}),
                config_file_path=str(self.directory_path / Paths.CONFIG_FILE),
                resource_name=resource_name,
            )

        return resources

    @staticmethod
    def _validate_resource_dir(name: str, resource_dir: Path) -> None:
        if not resource_dir.is_dir():
            raise RustyError(
                error=f"missing directory for resource '{name}'",
                file=str(resource_dir),
                help=f"create the directory `{resource_dir}/`",
            )

        for file in (Paths.DDL_TEMPLATE_FILE, Paths.STATE_FILE):
            if not (resource_dir / file).exists():
                raise RustyError(
                    error=f"missing '{file}' for resource '{name}'",
                    file=str(resource_dir / file),
                    help="create the file with a valid Jinja2 SQL template",
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
        source_dir = files(Paths.TEMPLATES_ANCHOR).joinpath(Paths.PROVIDER_DIR)

        target_dir = Path(f"{Paths.CONFIG_DIR}\\{Paths.PROVIDER_DIR}")
        target_dir.mkdir(parents=True, exist_ok=True)

        cls._copy_resources(source_dir, target_dir)
