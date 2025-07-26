"""Utility functions.

This module provides utility function for the pipeline run.
"""

import tomllib
import os
import re
import requests
import sqlparse
# from snowflake.connector import SnowflakeConnection
from collections import deque
from jinja2 import Environment, meta, UndefinedError, TemplateSyntaxError

from errors import DefinitionKeyError, DependencyError, FilePathError, ClientCredentialsError, OAuthTokenError, TemplateFileError




class Utils:
    """Contains functions designed to generate SQL queries for execution in Snowflake, following the dependency order defined in the YAML files."""

    def __init__(
            self, 
            resources_folder: str, 
            definitions_folder: str, 
            sf_app_client_id: str | None = None,
            sf_app_client_secret: str | None = None,
            sf_app_tenant_id: str | None = None,
            sf_app_scope: str | None = None,
            ):
        """Load the templates environments and list definitions files.

        Args:
            resources_folder (str): Path to the folder containing Snowflake resource templates 
                                  (SQL files with Jinja formatting).
            definitions_folder (str): Path to the folder containing Snowflake resource definitions 
                                    (YAML files).
            sf_app_client_id (str): Azure registered Snowflake app id.
            sf_app_client_secret (str): Azure registered Snowflake app secret.
            sf_app_tenant_id (str): Azure registered Snowflake app tenant.
            sf_app_scope (str): Azure registered Snowflake app scopes: `api://<sf_app_clint_id>/.default`.

        """
        try:
            self.resources_folder = resources_folder
            self.definitions_path = definitions_folder
            self.definitions_files = os.listdir(self.definitions_path)
            self.sf_app_client_id = sf_app_client_id
            self.sf_app_client_secret = sf_app_client_secret
            self.sf_app_tenant_id = sf_app_tenant_id
            self.sf_app_scope = sf_app_scope

        except Exception:
            raise FilePathError(definitions_folder, resources_folder)



    def render_templates(self, template:str, definition:dict, iac_action:str, obj_name:str = "NA") -> str:
        """Render the Jinja template of the resource.

        Args:
            template (str): The resouce sql template to render.
            definition (dict): The definition of the resource.
            iac_action (str): The type of execution iac_action to perform in Snowflake (e.g., "create", "alter", or "drop").
            obj_name (str): The name of the resouce object.

        Returns:
            str: The rendered SQL template as a string.

        """
        try:
            env = Environment()
            # Validate that all keys in the template are present in definition
            parsed_obj_template = env.parse(template)
            required_vars = meta.find_undeclared_variables(parsed_obj_template)
            missing_vars = [var for var in required_vars if var not in definition and var != "iac_action"]
            if missing_vars:
                raise TemplateFileError(obj_name, self.resources_folder, f"Missing variables: {missing_vars}")

            # Sanitze definition, removing SQL possible injection characters
            sanitized_definition = {
                k: str(v).replace(";", "").replace("--", "") if isinstance(v, str) else v
                for k, v in definition.items()
            }


            obj_template = env.from_string(template)
            sql = obj_template.render(
                iac_action=iac_action,
                **sanitized_definition,
                )
            # Clean the rendered multiline script from new lines 
            sql_clean = re.sub(r"\n+", "\n", sql)
            # Remove leading/trailing whitespaces and ";"
            sql_clean = sql_clean.strip() 
            sql_clean = sql_clean.strip(";") 

        except (KeyError, TemplateSyntaxError, UndefinedError) as e:
            raise TemplateFileError(obj_name, self.resources_folder, e)

        return sqlparse.format(sql_clean, reindent=True, keyword_case="upper")

    def dependencies_map(self) -> dict:
        """Create a topographic depencies map of the objects."""
        map = {}
        for file in self.definitions_files:
            # Load each definition yaml file as a dictionary
            file_path = os.path.join(self.definitions_path, file)
            with open(file_path) as f:
                definition = tomllib.load(f)

            if definition:
                # Get the object name from the definition dictionary
                # The object name is the key of the dictionary.
                object = "".join(definition.keys())

                # For each item in the definition create 
                # a combination of the object and it's name.
                # Example: "database::ajwa_presentation"
                for i in definition[object]:
                    try:
                        o_hash = f"{object}::{i['name']}"

                        # Check if the object definition has `depend_on` field
                        # Raise exception if as it's mandatory even if None
                        dependencies = i["depends_on"]

                        # Check if the object has any dependencies
                        if dependencies:

                            # For each dependency, the dependency object 
                            # and it's corresponding name is combined.
                            # Example: "role::bi_admin_role"
                            d_hash = [
                                f"{key}::{i}" 
                                for key, value in dependencies.items() 
                                for i in value
                            ]
                        else:
                            # If the object has no dependecies,
                            # an empty list is assigned.
                            d_hash = []
                        map[o_hash] = d_hash
                    except KeyError:
                        raise DefinitionKeyError("depends_on", obj_name=object, file=object)

        return map

    def dependencies_sort(self, map:dict):
        """Sorts the order in which the objects templates need to execute."""
        # Calculate in-degrees of all nodes
        try:
            in_degree = {node: 0 for node in map}
            for node in map:
                for neighbor in map[node]:
                    in_degree[neighbor] += 1
        except KeyError:  
            raise DependencyError(map)

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
            for neighbor in map[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:  # Add to queue if in-degree becomes 0
                    queue.append(neighbor)

        # Check if all nodes were processed
        if processed_count != len(map):
            return {
                "map":None,
                "message":f"There is a cyclical dependecy in the map: {map}."} 
        return {
            "map":topo_order[::-1],
            "message":"Dependecies map created",
        }
    


    def get_oauth_access_token(self) -> str:
        """Get access token for the snowflke connection."""
        try:
            client_id = self.sf_app_client_id
            client_secret = self.sf_app_client_secret
            tenant_id = self.sf_app_tenant_id
            toke_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            scope = [self.sf_app_scope]
        except KeyError as e:
            raise ValueError(f"Missing credentials variable: {e}") from e  # noqa: TRY003

        # OAuth request data
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        }

        try:
            response = requests.post(toke_url, data=data)
            response_data = response.json()
            if response.ok:
                return response_data.get("access_token")
        except Exception:
            raise OAuthTokenError(response=response)

    # def execute_rendered_template(self, connection:SnowflakeConnection, definition:dict, sql:str) -> None:
    #     """Execute rendered template."""
    #     cur = connection.cursor()
    #     if definition.get("database"):
    #         cur.execute(f"use database {definition['database']};")
    #     if definition.get("schema"):
    #         cur.execute(f"use schema {definition['schema']};")
    #     if definition.get("owner"):
    #         cur.execute(f"use role {definition['owner']};")
        
    #     cur.execute(sql)
    #     # Wait after execution if necesary, befor the next one.
    #     if definition.get("wait_time"):
    #         time.sleep(definition["wait_time"])

    def load_python_proc(self, file_path:str):
        """Zip and load python source procedure to internal stage."""
        #TODO: 
        pass