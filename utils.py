"""Utility functions.

This module provides utility function for the pipeline run.
"""

import yaml
import os
import re
import uuid
import requests
import boto3
import json
import time
from snowflake.connector import SnowflakeConnection
from collections import deque
from jinja2 import Environment, FileSystemLoader

from errors import DefinitionKeyError, DependencyError, FilePathError, ClientCredentialsError, OAuthTokenError




class Utils:
    """Contains functions designed to generate SQL queries for execution in Snowflake, following the dependency order defined in the YAML files."""

    def __init__(
            self, 
            resources_folder: str, 
            definitions_folder: str, 
            sf_app_client_id: str | None,
            sf_app_client_secret: str | None,
            sf_app_tenant_id: str | None,
            sf_app_scope: str | None,
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
            self.resources_env = Environment(loader=FileSystemLoader(f"{resources_folder}"))
            self.definitions_path = definitions_folder
            self.definitions_files = os.listdir(self.definitions_path)
            self.sf_app_client_id = sf_app_client_id
            self.sf_app_client_secret = sf_app_client_secret
            self.sf_app_tenant_id = sf_app_tenant_id
            self.sf_app_scope = sf_app_scope

        except Exception:
            raise FilePathError(definitions_folder, resources_folder)


    def assign_pipeline_tag_id(self) -> None:
        """Go through all the definitions of snowflake objects and checks if they have the `object_id_tag`. It will assign one if there is none."""
        for file in self.definitions_files:
            modified = False
            # Load each definition yaml file as a dictionary
            file_path = os.path.join(self.definitions_path, file)
            with open(file_path) as f:
                definition = yaml.safe_load(f)

                if definition:
                    # Convert the dictiory key into a string
                    object = "".join(definition.keys())

                    for i in definition[object]:
                        try:
                            # Check if object has a name and is not empy
                            if i["name"] is not None:
                                obj_name = i["name"]
                            else:
                                raise DefinitionKeyError("name", file=object)
                        except KeyError:
                            raise DefinitionKeyError("name", file=object)

                        if "object_id_tag" in i:
                            if not i["object_id_tag"]:
                                i["object_id_tag"] = str(uuid.uuid3(uuid.NAMESPACE_DNS, obj_name))
                                modified = True
                        else:
                            raise DefinitionKeyError("object_id_tag", file=object, obj_name=obj_name)
                    
                    # Write back the modified dictionary to the YAML file
                    if modified:
                        with open(f"{self.definitions_path}/{file}", "w") as f:
                            yaml.dump(
                                definition,
                                f,
                                default_flow_style=False,
                                allow_unicode=True,
                                sort_keys=False,
                            )
                            print(f"Updated file: {file}")


    def render_templates(self, template_file: str, definition: dict, iac_action: str, new_name: str = None, old_name: str = None) -> str:
        """Render the Jinja template based on the provided parameters.

        Args:
            template_file (str): The name of the template file to render.
            definition (dict): The dictionary loaded from the definitions YAML file.
            iac_action (str): The type of execution iac_action to perform in Snowflake (e.g., "create", "alter", or "drop").
            new_name (str, optional): The new name of the Snowflake object, taken from the YAML file definition. 
                                    Used for renaming objects. Defaults to None.
            old_name (str, optional): The current name of the Snowflake object, taken from its state in Snowflake. 
                                    Used for renaming objects. Defaults to None.

        Returns:
            str: The rendered SQL template as a string.

        """
        template = self.resources_env.get_template(template_file)

        sql = template.render(new_name=new_name, old_name=old_name, iac_action=iac_action,**definition)

        # Removing extra new line from the template output
        return re.sub(r"\n+", "\n", sql)




    def dependencies_map(self) -> dict:
        """Create a topographic depencies map of the objects."""
        map = {}
        for file in self.definitions_files:
            # Load each definition yaml file as a dictionary
            file_path = os.path.join(self.definitions_path, file)
            with open(file_path) as f:
                definition = yaml.safe_load(f)

            if definition:
                # Convert the dictiory key into a string
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

    def execute_rendered_template(self, connection:SnowflakeConnection, definition:dict, sql:str) -> None:
        """Execute rendered template."""
        cur = connection.cursor()
        if definition.get("database"):
            cur.execute(f"use database {definition['database']};")
        if definition.get("schema"):
            cur.execute(f"use schema {definition['schema']};")
        if definition.get("owner"):
            cur.execute(f"use role {definition['owner']};")
        
        cur.execute(sql)
        # Wait after execution if necesary, befor the next one.
        if definition.get("wait_time"):
            time.sleep(definition["wait_time"])
