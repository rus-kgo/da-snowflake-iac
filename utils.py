"""Utility functions.

This module provides utility function for the pipeline run.
"""

import tomllib
import os
import re
import sqlparse
from sqlalchemy import create_engine, Connection
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from collections import deque
from jinja2 import Environment, meta, UndefinedError, TemplateSyntaxError
from rich.console import Console

from errors import (
    DefinitionKeyError,
    DependencyError,
    FilePathError,
    TemplateFileError,
    SQLExecutionError,
)


class Utils:
    """Contains functions designed to generate SQL queries for execution in Snowflake, following the dependency order defined in the YAML files."""

    def __init__(
        self,
        resources_path:str,
        definitions_path:str,
    ):
        """Load the templates environments and list definitions files.

        Args:
            resources_path (str): Path to the folder containing Snowflake resource templates
                                  (SQL files with Jinja formatting).
            definitions_path (str): Path to the folder containing Snowflake resource definitions
                                    (YAML files).

        """
        try:
            self.resources_path = resources_path
            self.definitions_path = definitions_path
            self.definitions_files = os.listdir(self.definitions_path)
            self.console = Console()
        except Exception as err:
            raise FilePathError(definitions_path, resources_path) from err

    def clean_env_vars(self, string):
        """Make sure environemnt values are of a correct type."""
        if string.isdigit():
            return int(string)
        if string.lower() in {"true", "false"}:
            return string.lower() == "true"
        if string is None or string in {"None",""}:
            return None
        return None

    def render_templates(
        self,
        template: str,
        definition: dict = None,
        iac_action: str = None,
        resource_name: str = None,
    ) -> str:
        """Render the Jinja template of the resource.

        Args:
            template (str): The resouce sql template to render.
            definition (dict, optional): The definition of the resource.
            iac_action (str, optional): The type of execution iac_action to perform in Snowflake (e.g., "create", "alter", or "drop").
            resource_name (str, optional): The name of the resource.

        Returns:
            str: The rendered SQL template as a string.

        """
        try:
            env = Environment()
            # Validate that all keys in the template are present in definition
            parsed_rsc_template = env.parse(template)
            required_vars = meta.find_undeclared_variables(parsed_rsc_template)
            missing_vars = [
                var
                for var in required_vars
                if var not in definition and var != "iac_action"
            ]
            if missing_vars:
                raise TemplateFileError(
                    resource_name,
                    self.resources_path,
                    f"Definition is missing variables: {missing_vars}",
                )

            # Sanitze definition, removing SQL possible injection characters
            sanitized_definition = {
                k: str(v).replace(";", "").replace("--", "")
                if isinstance(v, str)
                else v
                for k, v in definition.items()
            }

            rsc_template = env.from_string(template)
            sql = rsc_template.render(
                iac_action=iac_action,
                **sanitized_definition,
            )
            # Clean excrea new lines.
            sql_clean = re.sub(r"\n+", "\n", sql)
            # Remove leading/trailing whitespaces and semicolon.
            sql_clean = sql_clean.strip()
            sql_clean = sql_clean.strip(";")

        except (KeyError, TemplateSyntaxError, UndefinedError) as e:
            raise TemplateFileError(resource_name, self.resources_path, e) from e

        return sqlparse.format(sql_clean, reindent=True, keyword_case="upper")

    def dependencies_map(self) -> dict:
        """Create a topographic depencies map of the resource."""
        d_map = {}
        for file in self.definitions_files:

            file_path = os.path.join(self.definitions_path, file)
            with open(file_path) as f:
                definition = tomllib.load(f)

            if definition:
                # Get the resource name from the definition dictionary
                # The resource name is the key of the dictionary.
                resource = "".join(definition.keys())

                # For each item in the definition create
                # a combination of the resource and it's name.
                # Example: "database::ajwa_presentation"

                for i in definition[resource]:
                    o_hash = f"{resource}::{i['name']}"

                    # Check if the resource definition has `depend_on` field
                    # Raise exception if as it's mandatory even if None
                    dependencies:dict = i.get("depends_on","missing")

                    if dependencies == "missing":
                        raise DefinitionKeyError(
                            keys=["depends_on"],
                            name=resource,
                            file=resource,
                        )

                    # Check if the resource has any dependencies
                    if dependencies != "missing":
                        # For each dependency, the dependency resource
                        # and it's corresponding name is combined.
                        # Example: "role::bi_admin_role"
                        d_hash = [
                            f"{key}::{i}"
                            for key, value in dependencies.items()
                            for i in value
                        ]
                    else:
                        # If the resource has no dependecies,
                        # an empty list is assigned.
                        d_hash = []
                    d_map[o_hash] = d_hash

        return d_map

    def dependencies_sort(self, d_map: dict) -> list:
        """Sorts the order in which the resources templates need to execute."""
        # Calculate in-degrees of all nodes
        try:
            # Calculate in-degrees of all nodes, ensuring neighbors are initialized
            in_degree = {}
            for node, neighbors in d_map.items():
                # Ensure the node exists in the in_degree map
                in_degree.setdefault(node, 0)
                for neighbor in neighbors:
                    in_degree[neighbor] = in_degree.get(neighbor, 0) + 1

            # Add nodes with in-degree 0 to the queue
            queue = deque([node for node in in_degree if in_degree[node] == 0])

            # Process nodes in the queue
            topo_order = []
            processed_count = 0  # To track processed nodes
            while queue:
                current = queue.popleft()
                topo_order.append(current)
                processed_count += 1

                # Reduce the in-degree of neighbors
                for neighbor in d_map[current]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:  # Add to queue if in-degree becomes 0
                        queue.append(neighbor)

        except KeyError as err:
            raise DependencyError(d_map) from err

        # Check if all nodes were processed
        if processed_count != len(d_map):
            raise DependencyError(d_map, is_cyclical=True)

        return topo_order[::-1]

    def create_db_sys_connection(self, database_system: str):
        """Create SQL connection for query execution."""
        # Load the engine connection arguments of the database system.
        try:
            with open(self.resources_path, "rb") as f:
                db_sys_config = tomllib.load(f)
                db_sys_engine = db_sys_config.get(database_system, {}).get("engine", {})
        except FileNotFoundError as err:
            raise FilePathError(self.resources_path) from err

        # Extract URL and connect_args separately
        url = db_sys_engine.get("sqlalchemy.url")
        connect_args = db_sys_engine.get("sqlalchemy.connect_args", {})

        # Match the environment keys with the db_sys_engine config
        # e.g. SNOWFLAKE_ENGINE_SQLALCHEMY_CONNECT_ARGS_ACCOUNT
        prefix = f"{database_system.upper()}_ENGINE_SQLALCHEMY_CONNECT_ARGS_"
        try:
            for key, value in os.environ.items():
                if key.startswith(prefix):
                    clean_value = self.clean_env_vars(value)

                    # Skip the prefix, get only the relevant keys
                    nested_keys = key[len(prefix) :].lower()

                    if nested_keys in connect_args:
                        connect_args[nested_keys] = clean_value

        except KeyError as e:
            raise ValueError(f"Missing keys in databse system config: {e}") from e  # noqa: TRY003

        # Get values for key pair authentification
        private_key_path: str = db_sys_engine["sqlalchemy.connect_args"].get(
            "private_key_path",
            None,
        )
        private_key: str = db_sys_engine["sqlalchemy.connect_args"].get(
            "private_key",
            "",
        )
        private_key_passphrase: str = db_sys_engine["sqlalchemy.connect_args"].get(
            "private_key_passphrase",
            "",
        )

        if private_key_passphrase:
            private_key_passphrase = private_key_passphrase.encode()
        else:
            private_key_passphrase = None

        p_key = None
        if private_key_path:
            with open(private_key_path, "rb") as key_file:
                p_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=private_key_passphrase,
                    backend=default_backend(),
                )

        elif private_key:
            # When your secret private key in env is escaped (like -----BEGIN PRIVATE KEY-----\nMIIEvg...)
            private_key_str = private_key.replace("\\n", "\n")

            p_key_bytes = private_key_str.encode()
            p_key = serialization.load_pem_private_key(
                p_key_bytes,
                password=private_key_passphrase,
                backend=default_backend(),
            )

        if p_key:
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            connect_args["private_key"] = pkb

            # Remove these from connect_args as they're not needed by Snowflake
            connect_args.pop("private_key_path", None)
            connect_args.pop("private_key_passphrase", None)

        engine = create_engine(url, connect_args=connect_args, echo=False)

        return engine.connect()

    def execute_rendered_sql_template(
        self,
        conn: Connection,
        sql: str,
    ) -> None:
        """Execute rendered templates using SQL database connection."""
        try:
            conn.exec_driver_sql(sql)
        except Exception as err:
            raise SQLExecutionError(
                error=err,
                sql=sql,
                ) from err
        else:
            self.console.print("[bold green3]\nSQL EXECUTION SUCCESSFULL[/bold green3]")

    def zip_python_proc(self, file_path: str):
        """Zip python source code for a procedure in a database."""
        # TODO:
        pass


if __name__ == "__main__":
    Utils(
        resources_path="resources.toml",
        definitions_path="definitions",
    ).create_db_sys_connection(database_system="sqlite")
