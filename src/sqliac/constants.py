from enum import StrEnum
from pathlib import Path


class TemplateType(StrEnum):
    """Defines types of SQL Jinja templates."""

    DDL = "ddl"
    STATE = "state"


class DDLCommand(StrEnum):
    """Defines the SQL DDL command."""

    CREATE = "create"
    ALTER = "alter"
    DROP = "drop"
    ALL = "all"
    NO_ACTION = "no_action"

    @classmethod
    def executable(cls) -> tuple[StrEnum, ...]:
        return (cls.CREATE, cls.ALTER, cls.DROP)


class IacAction(StrEnum):
    """Defines the action to perform."""

    APPLY = "apply"
    DESTROY = "destroy"


class RunMode(StrEnum):
    """Defines the program run mode."""

    DRY_RUN = "dry_run"
    LIVE_RUN = "live_run"


class Paths:
    """Defines the required file paths as Path objects."""

    CONFIG_DIR = Path(".sqliac")
    PROVIDER_DIR = Path("provider")
    PROVIDER_CONFIG_FILE = CONFIG_DIR / PROVIDER_DIR / "config.toml"
    PROVIDER_CREDENTIALS_FILE = CONFIG_DIR / PROVIDER_DIR / "credentials.toml"
    DEFINITIONS_DIR = Path("definitions")
    TEMPLATES_ANCHOR = "sqliac.templates"
    DEPENDENCIES_FILE = Path("dependencies.dot")

    @classmethod
    def ddl_template_file(cls, resource_type: str) -> Path:
        """Returns the DDL template file path for a given resource type."""
        return cls.CONFIG_DIR / cls.PROVIDER_DIR / resource_type / "ddl_template.sql"

    @classmethod
    def state_file(cls, resource_type: str) -> Path:
        """Returns the state file path for a given resource type."""
        return cls.CONFIG_DIR / cls.PROVIDER_DIR / resource_type / "state.sql"
