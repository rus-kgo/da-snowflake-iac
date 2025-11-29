from utils import Utils  # noqa: D100
import tomllib
from rich.console import Console
from rich.syntax import Syntax

from sqlglot import parse_one
from sqlglot.errors import ParseError

class LocalTest:
    """Local test class to test the resources templates."""
    def __init__(
            self,
            db_system:str,
            resource_type:str,
            resource_name:str,
            ):
        """Initialize LocalTest with database system, resource type, and resource name."""
        self.db_system = db_system
        self.resource_type = resource_type
        self.resource_name = resource_name

        with open("resources.toml","rb") as f:
            config = tomllib.load(f)
            self.db_sys_resources = config[self.db_system]["resources"]


    def test_template_query(self,iac_action:str):
        """Load resource and print out the output for testing."""
        utils = Utils(
            resources_path="resources.toml",
            definitions_path="",
        )

        sql = utils.render_templates(
            template=self.db_sys_resources[self.resource_type]["template"],
            definition=self.db_sys_resources[self.resource_type]["definition"],
            name=self.resource_name,
            iac_action=iac_action,
        )

        try:
            # Parse SQL with databse system dialect
            parsed = parse_one(sql, read=self.db_system)

            # Optionally, regenerate SQL to check completeness
            regenerated = parsed.sql(dialect=self.db_system)

            print("SQL from definition template:\n")
            pretty_sql = Syntax(sql, "sql", theme="monokai", line_numbers=False)
            Console().print(pretty_sql)

            print("\nParser re-generated SQL for completness:\n")
            pretty_sql = Syntax(regenerated, "sql", theme="monokai", line_numbers=False)
            Console().print(pretty_sql)

        except ParseError as e:
            print(f"SQL Parse Error: {str(e)}")
        except Exception as e:
            print(f"Validation Error: {str(e)}")

    def test_status_query(self):
        """Test the resource status query."""
        utils = Utils(
            resources_path="resources.toml",
            definitions_path="",
        )

        sql = utils.render_templates(
            template=self.db_sys_resources[self.resource_type]["state_query"],
            name=self.resource_name,
        )

        try:
            # Parse SQL with databse system dialect
            parsed = parse_one(sql, read=self.db_system)

            # Optionally, regenerate SQL to check completeness
            regenerated = parsed.sql(dialect=self.db_system)

            print("SQL from status query template:\n")
            pretty_sql = Syntax(sql, "sql", theme="monokai", line_numbers=False)
            Console().print(pretty_sql)

            print("\nParser re-generated SQL for completness:\n")
            pretty_sql = Syntax(regenerated, "sql", theme="monokai", line_numbers=False)
            Console().print(pretty_sql)

        except ParseError as e:
            print(f"SQL Parse Error: {str(e)}")
        except Exception as e:
            print(f"Validation Error: {str(e)}")




if __name__ == "__main__":
    local_test = LocalTest(
        resource_name="table_name",
        resource_type="table",
        db_system="sqlite",
    )
    local_test.test_template_query(
        iac_action="create",
    )
    local_test.test_status_query()
