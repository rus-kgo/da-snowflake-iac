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
from collections import deque
from jinja2 import Environment, FileSystemLoader

from errors import DefinitionKeyError, DependencyError, FilePathError, ClientCredentialsError, OAuthTokenError




class Utils:
    """Contains functions designed to generate SQL queries for execution in Snowflake, following the dependency order defined in the YAML files."""

    def __init__(
            self, 
            resources_folder: str, 
            definitions_folder: str, 
            aws_access_key_id:str, 
            aws_secret_access_key:str, 
            aws_region_name:str, 
            aws_role_arn:str, 
            snowflake_client_secret:str,
            ):
        """Load the templates environments and list definitions files.

        Args:
            resources_folder (str): Path to the folder containing Snowflake resource templates 
                                  (SQL files with Jinja formatting).
            definitions_folder (str): Path to the folder containing Snowflake resource definitions 
                                    (YAML files).
            aws_access_key_id (str): AWS account access key id.
            aws_secret_access_key (str): AWS account access secret key.
            aws_region_name (str): AWS account region.
            aws_role_arn (str): AWS role with access to the Secrets Manager.
            snowflake_client_secret (str): Name of the snowflake client secret stored in AWS Secrets Manager.

        """
        try:
            self.resources_env = Environment(loader=FileSystemLoader(f"{resources_folder}"))
            self.definitions_path = definitions_folder
            self.definitions_files = os.listdir(self.definitions_path)
            self.aws_access_key_id = aws_access_key_id
            self.aws_secret_access_key = aws_secret_access_key
            self.aws_region_name = aws_region_name
            self.aws_role_arn = aws_role_arn
            self.snowflake_client_secret = snowflake_client_secret

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
                            obj_name = i["name"]
                        except KeyError:
                            raise DefinitionKeyError("name", file=object)
                        # Check if object has a name and is not empy

                        if obj_name:
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


    def render_templates(self, template_file: str, definition: dict, action: str, new_name: str = None, old_name: str = None) -> str:
        """Render the Jinja template based on the provided parameters.

        Args:
            template_file (str): The name of the template file to render.
            definition (dict): The dictionary loaded from the definitions YAML file.
            action (str): The type of execution action to perform in Snowflake (e.g., "create", "alter", or "drop").
            new_name (str, optional): The new name of the Snowflake object, taken from the YAML file definition. 
                                    Used for renaming objects. Defaults to None.
            old_name (str, optional): The current name of the Snowflake object, taken from its state in Snowflake. 
                                    Used for renaming objects. Defaults to None.

        Returns:
            str: The rendered SQL template as a string.

        """
        template = self.resources_env.get_template(template_file)

        sql = template.render(new_name=new_name, old_name=old_name, action=action,**definition)

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
                        raise DefinitionKeyError("depends_on", "name", file=object)

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
    

    def _get_client_credentials(self) -> dict:
        """Get client app credentials from AWS Secret Manager."""
        try:
            sts_client = boto3.client(
                "sts",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region_name,
                ) 

            # Accume the specific role that has access to the Secret Manager
            assumed_role_object = sts_client.assume_role(
                RoleArn=self.aws_role_arn,
                RoleSessionName="da-snowflake-iac",
            )

            # Extract temporary credentials from the assumed role
            credentials = assumed_role_object["Credentials"]

            # Use temporary credentials to create a new boto3 session
            aws_session = boto3.Session(
                aws_access_key_id = credentials["AccessKeyId"],
                aws_secret_access_key = credentials["SecretAccessKey"],
                aws_session_token = credentials["SessionToken"],
                region_name = self.aws_region_name,
            )

            # Create a Secret Manager client
            secrets_manager = aws_session.client("secretsmanager")

            get_secret_value_response = secrets_manager.get_secret_value(SecretId=self.snowflake_client_secret) 

            return json.loads(get_secret_value_response["SecretString"])
        except Exception as e:
            raise ClientCredentialsError(e)


    def get_oauth_access_token(self) -> str:
        """Get access token for the snowflke connection."""
        credentials = self._get_client_credentials()
        try:
            client_id = credentials["clientId"]
            client_secret = credentials["clientSecret"]
            tenant_id = credentials["tenantId"]
            toke_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            scope = credentials["scope"]
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
            return response_data.get("access_token")
        except Exception:
            raise OAuthTokenError(response=response)
