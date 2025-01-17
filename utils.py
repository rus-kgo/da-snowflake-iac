"""Utility functions and classes for the pipeline.

This module provides:
- FasterThanLightError: exception when FTL speed is calculated;
- calculate_speed: calculate speed given distance and time.
"""

import yaml
import os
import re
import uuid
from collections import deque
from jinja2 import Environment, FileSystemLoader

from errors import DefinitionKeyError, DependencyError, FilePathError




class Utils:
    """Contains functions designed to generate SQL queries for execution in Snowflake, following the dependency order defined in the YAML files."""

    def __init__(self, resources_folder: str, definitions_folder: str):
        """Load the templates environments and list definitions files.

        Args:
            resources_folder (str): Path to the folder containing Snowflake resource templates 
                                  (SQL files with Jinja formatting).
            definitions_folder (str): Path to the folder containing Snowflake resource definitions 
                                    (YAML files).

        """
        # TODO: make sure the file paths work for workflows


        try:
            self.resources_env = Environment(loader=FileSystemLoader(f"{resources_folder}"))
            self.definitions_path = f"{definitions_folder}"
            self.definitions_files = os.listdir(self.definitions_path)
        except Exception:
            raise FilePathError(definitions_folder, resources_folder, cwd=self.cwd)


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



    def state_comparison(self, new_state:dict, old_state:dict) -> dict:
        """Compare the state of the object in Snowflake against the state of the object .

        In the YAML config file and return the definition for altering the object 
        if there is a difference.

        Args:
           new_state (dict): The current state of the object from Snowflake.
           old_state (dict): The desired state of the object from the YAML config.

        Returns:
            dict: A dictionary containing the differences, formatted for alteration.

        """
        if new_state != old_state:
            unique_keys = new_state.keys() | old_state.keys()

            differences = {
                key: {"new_value": new_state.get(key), "old_value": old_state.get(key)} 
                for key in unique_keys 
            }

            return {
                    key: differences[key]["new_value"]
                    for key in differences
                }
        return {}


    def dependencies_map(self):
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

    def dependencies_sort(self, map):
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
    

    def snowflake_state(self, cursor, object, object_id_tag) -> dict:
        
        # TODO: replace underscores with spaces of the objects

        # describe_sql = f"""
        #    DESC {object} {object_name}
        # """

        # show_sql : f"""
        #     SHOW {object}S like {object_name}
        # """

        # full_description = session.sql(describe_sql)

        # if full_description == "desc output":

        #     return  full_description
        # else:
        #     short_description = session.sql(show_sql)
        #     return short_description
        # output = {
        #     'name': 'test_inter', 
        #     'type': 'external_oauth', 
        #     'enabled': 'true', 
        #     'external_oauth_type': 'azure', 
        #     'external_oauth_issuer': ['http://issuer1', 'issuer2'], 
        #     'external_oauth_token_user_mapping_claim': ['http://token'], 
        #     'external_oauth_snowflake_user_mapping_attribute': 3, 
        #     'external_oauth_jws_keys_url': ['key_url'], 
        #     'external_oauth_blocked_roles_list': ['blocked'], 
        #     'external_oauth_allowed_roles_list': ['allowed'],
        #     'external_oauth_rsa_public_key': 'pubkey', 
        #     'external_oauth_rsa_public_key_2': 'pubke2', 
        #     'external_oauth_audience_list': ['audience1', 'audience2'], 
        #     'external_oauth_any_role_mode': 'true', 
        #     'external_oauth_scope_delimiter': ',', 
        #     'external_oauth_scope_mapping_attribute': 'whatever', 
        #     'comment': None, 
        #     'tag': 'd191164a-7813-45f2-aba9-400f3bd31397'
        #     }

        output = {
            "name": "my_db",
            "comment": "to be used for aws step functions",
            "object_id_tag": "792c2a9c-2812-39ed-99b4-1c182450260f",
            }

        if output["object_id_tag"] == object_id_tag:
            return output
        return {}