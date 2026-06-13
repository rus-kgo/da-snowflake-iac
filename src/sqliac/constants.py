from enum import StrEnum  # noqa: D100


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
    def executable(cls) -> tuple[DDLCommand, ...]:  # noqa: D102
        return (cls.CREATE, cls.ALTER, cls.DROP)


class IacAction(StrEnum):
    """Defines the action to perform."""

    APPLY = "apply"
    DESTROY = "destroy"


class RunMode(StrEnum):
    """Defines the program run mode."""

    DRY_RUN = "dry_run"
    LIVE_RUN = "live_run"


class Paths(StrEnum):
    """Defines the required file paths."""

    CONFIG_DIR = ".sqliac"
    CONFIG_FILE = "config.toml"
    CREDENTIALS_FILE = "credentials.toml"
    PROVIDER_DIR = "provider"
    DEFINITIONS_DIR = "definitions"
    DDL_TEMPLATE_FILE = "ddl_template.sql"
    STATE_FILE = "state.sql"
    TEMPLATES_ANCHOR = "sqliac.templates"
    DEPENDENCIES_FILE = "dependencies.dot"
