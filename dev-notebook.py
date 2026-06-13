import marimo

__generated_with = "0.19.1"
app = marimo.App(width="medium")


@app.cell
def _():
    import tomllib
    import re
    from dataclasses import dataclass, field
    from collections import deque
    from icecream import ic
    return dataclass, field, ic, tomllib


@app.cell
def _(tomllib):
    with open(r"src\\sqliac\\templates\\provider\\config.toml", "rb") as f:
        data = tomllib.load(f)
    return (data,)


@app.cell
def _(data):
    data
    return


@app.cell
def _(data):
    from typing import Any, get_origin, get_args

    print(get_origin(data))
    print(get_args(data))
    return


@app.cell
def _(data):
    list(data.get("snowflake").get("resources",{}).keys())
    return


@app.cell
def _(dataclass, field):
    @dataclass
    class ExecutionPlan:
        """Encapsulates the execution plan for all resources.

        Attributes:
            dependency_graph: Dict mapping resource -> set of dependencies.
                             e.g., {"database::my_db": {"role::admin"}}
            reverse_dependency_graph: Dict mapping resource -> set of dependents.
                                     e.g., {"role::admin": {"database::my_db"}}
        """

        success: bool
        dependency_graph: dict[str, set[str]] = field(default_factory=dict)
        reverse_dependency_graph: dict[str, set[str]] = field(default_factory=dict)
        errors: list[str] = field(default_factory=list)

        @property
        def has_issues(self) -> bool:
            """Check if there are any errors or warnings."""
            return bool(self.errors)
    return


@app.cell
def _(ic):
    def validate_dependencies(dependency_graph: dict[str, set[str]]) -> None:
        """Validate that all declared dependencies exist.

        Args:
            dependency_graph: The dependency graph to validate

        Raises:
            DependencyError: If any dependencies are missing
        """
        errors=[]
        # Collect all resource IDs that exist
        existing_resources = set(dependency_graph.keys())

        referenced_resources = set().union(*dependency_graph.values())

        # Find missing resources
        missing = referenced_resources - existing_resources

        if missing:
            ic(missing)
            blocks = []

            for item in missing:
                rsc, name = item.split("::")

                block = f"""\
            --> {rsc}.yml
            |
            1 | [[{rsc}]]
            2 | name = "{name}"
            |       ^^^ missing definition
            |
            """

                blocks.append(block)

            errors.append(
                "\nerror: invalid dependency reference"
                "  |\n"
                "  = help: missing resources:\n\n" + "\n".join(blocks)
            )
            print("wrong" in errors[0])
    return (validate_dependencies,)


@app.cell
def _():
    rsc_definitions = {"role": [{"name": "admin", "depends_on": {}}]}
    return


@app.cell
def _(validate_dependencies):
    dependency_graph = {"role::admin": set(), "database::my_db": {"role::wrong"}}
    validate_dependencies(dependency_graph)
    return


@app.cell
def _(existing, ic, referenced):
    ic(existing)
    ic(referenced)
    ic(referenced - existing)
    return


@app.cell
def _(dataclass):
    from errors import RustyError


    @dataclass
    class IacAction:
        """Infrastructure-as-Code action configuration.

        Defines which SQL operations are supported for a resource type.

        Attributes:
            create: SQL action keyword for creation (e.g., "CREATE", "CREATE OR ALTER")
            alter: SQL action keyword for modification (e.g., "ALTER", "CREATE OR ALTER", empty string)
            drop: SQL action keyword for deletion (e.g., "DROP")

        Example:
            ```toml
            [snowflake.resources.table.iac_action]
            create = "CREATE OR ALTER"
            alter = "CREATE OR ALTER"
            drop = "DROP"
            ```
        """

        create: str
        alter: str
        drop: str

        def __post_init__(self):
            """Check the values are of a string typ."""
            if (
                not isinstance(self.create, str)
                or not isinstance(self.alter, str)
                or not isinstance(self.drop, str)
            ):
                raise RustyError(
                    error="iac action values must be a string value",
                    file="resources.toml",
                    help="""review the `iac_action` values, see example:
        ```toml
        [snowflake.resources.table.iac_action]
        create = "CREATE OR ALTER"
        alter = "CREATE OR ALTER"
        drop = "DROP"
        ```
    """,
                )
    return (IacAction,)


@app.cell
def _(IacAction, IacActionError):
    try:
        action = IacAction(create=123, alter="correct", drop="correct")
    except IacActionError as e:
        print(f"error: {e}")
    return


if __name__ == "__main__":
    app.run()
