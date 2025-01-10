import yaml
import os
import re
import uuid
from collections import deque
from jinja2 import Environment, FileSystemLoader


class Utils():

    def __init__(self, resources_path:str, definitions_path:str):
        """
        The fucntions included in this class are desinged to generate 
        SQL queries to be executed in Sowflake following the depencies order 
        defined in the yaml files. 

        Parameters:
            `resources_path`
                The path to the folder containg Snowfalke resources templates using SQL files with Jinja formating.
            `definitions_path`
                The path to the folder containg Snowfalke resources definitions using yaml files.
        """

        # TODO: make sure the file paths work for workflows

        self.resources_env = Environment(loader=FileSystemLoader(resources_path))
        self.definitions_path = definitions_path
        self.definitions_files = os.listdir(definitions_path)


    def assign_pipeline_tag_id(self) -> None:
        """
        The function goes through all the definitions of snowflake objects 
        and checks if they have the `object_id_tag`.  
        It will assign one if there is none.
        """

        # Check if pipeine_id_tag exists
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
                            if not i['object_id_tag']:
                                i['object_id_tag'] = str(uuid.uuid3(uuid.NAMESPACE_DNS, i['name']))

                                modified = True
                                print(f"Updated {i['name']} with `object_id_tag`: {i['object_id_tag']}")

                        except KeyError:
                            raise Exception(f"Definition `{i['name']}` in the `{file}` is missing the `object_id_tag` field.")
                    
                    # Write back the modified dictionary to the YAML file
                    if modified:
                        with open(f'{self.definitions_path}/{file}', 'w') as f:
                            yaml.dump(
                                definition,
                                f,
                                default_flow_style=False,
                                allow_unicode=True,
                                sort_keys=False,
                            )
                            print(f"Updated file: {file}")


    def render_templates(self, template_file:str, definition:dict, action:str, new_name:str=None, old_name:str=None) -> str:  
        """
        Thie function will render the Jinja template.

        Parameters:
            `template_file`
                The name of the template file
            `definition`
                The dictionary loaded from definitions yaml file
            `action`
                The type of execution action to perfomr in Snowflake: create, alter or drop.
            `new_name`
                Name of the Snowflake object taken from the yaml file definition. Used for re-naming objects
            `old_name`
                Name of the Snowflake object taken from the object state on Snowflake. Used for re-naming objects
        """

        template = self.resources_env.get_template(template_file)

        sql = template.render(new_name=new_name, old_name=old_name, action=action,**definition)

        # Removing extra new line from the template output
        sql = re.sub(r'\n+', '\n', sql)
        # Step 1: Replace multiple spaces with a single space

        return sql


    def state_comparison(self, new_state:dict, old_state:dict) -> dict:
        """
        This function will compare the state of the object in Snowflake 
        against the state of the object in the yaml config file of that object
        and return the definition for alteration of the object if there is a difference.
        """
        if new_state != old_state:
            unique_keys = new_state.keys() | old_state.keys()

            differences = {
                key: {'new_value': new_state.get(key), 'old_value': old_state.get(key)} 
                for key in unique_keys 
            }

            alter_definition = {
                    key: differences[key]['new_value']
                    for key in differences
                }

            return alter_definition
        else:
            return {}


    def dependencies_map(self):
        """
        This function creates a topographic depencies map of the objects.
        """

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
                    o_hash = f"{object}::{i['name']}"
                    try:
                        # Check if the object definition has `depend_on` field
                        # Raise exception if as it's mandatory even if None
                        dependencies = i['depends_on']

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
                        raise Exception(f"Definition named `{i['name']}` in the `{file}` is missing the `depends_on` field.")

        return map

    def dependencies_sort(self, map):
        """
        Sorts the order in which the objects templates need to execute;
        """

        # Calculate in-degrees of all nodes
        try:
            in_degree = {node: 0 for node in map}
            for node in map:
                for neighbor in map[node]:
                    in_degree[neighbor] += 1
        except:
            raise Exception(f"There is an incorrect dependecy in the map {map}, make sure the objects names are correct.")

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
            "message":"Dependecies map created"
        }
    

    def snowflake_state(self, session, object, object_id_tag) -> dict:
        
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
            'name': 'db_rus_kgo',
            'comment': 'to be used for aws step functions',
            'object_id_tag': '82e28c10-a75a-49ba-99a2-ddd4664459ab'
            }

        if output['object_id_tag'] == object_id_tag:
            return output
        else:
            return {}