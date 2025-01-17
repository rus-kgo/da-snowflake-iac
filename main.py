"""Main entrance point of the pipeline.

This module provides:
- main: main function that orchestrates the pipeline;
"""

import yaml 
import os
import re
import snowflake.connector as sc

from utils import Utils
from errors import DefinitionKeyError, TemplateFileError, SnowflakeConnectionError

RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"


def get_private_key():
    pass




def main():
    """Entry point of the program."""
    try:
        private_key = b'<byites>'
        private_key_file_pwd = '<password>'

        conn_params = {
            'account': os.environ['SNOWFLAKE_ACCOUNT'],
            'user': '<user>',
            'private_key': private_key,
            'private_key_pwd':private_key_file_pwd,
            'warehouse': '<warehouse>',
            'database': '<database>',
            'schema': '<schema>'
        }
        # ctx = sc.connect(**conn_params)

        sf_cursor = "" #ctx.cursor()

    except Exception as e:
        raise SnowflakeConnectionError(error=e, conn_params=conn_params)

    definitions_folder = "/snowflake-iac/definitions"
    resources_folder = "/snowflake-iac/resources"
    run_mode = "create-or-alter" # "create-or-alter", "destroy"
    dry_run = True

    utils = Utils(
        definitions_folder=definitions_folder,
        resources_folder=resources_folder,
    )

    # First check if all definitions have their tags, assign a tag if not
    utils.assign_pipeline_tag_id()

    # Map the dependencies of all the definitions in the yaml files
    map = utils.dependencies_map()

    # Do topographic sorting of the dependecies
    sorted_map = utils.dependencies_sort(map)["map"]

    definitions_path = os.path.join(os.getcwd(), definitions_folder)

    print(f"{CYAN}account: {conn_params['account']}{RESET}\n")
    for i in sorted_map:
        object, object_name = i.split("::")

        file_path = os.path.join(definitions_path, f"{object}.yml")

        try:
            with open(file_path) as f:
                definition = yaml.safe_load(f)
        except FileNotFoundError:
            raise DefinitionKeyError(object) 
        
        try:
            for d_state in definition[object]:
                # This is for benefit of following the sorter order
                if d_state["name"] == object_name:

                    # Here we are getting the tag to compare it with the snowfalke object instead o using the name
                    object_id_tag = d_state["object_id_tag"]

                    # Making sure the object is correctly named `database role` instead of `database_role`
                    sf_object = re.sub(r"_", " ", object)

                    # Run snowflake function to retrieve the object details as dictionary
                    sf_state = utils.snowflake_state(cursor=sf_cursor, object=sf_object, object_id_tag=object_id_tag)

                    if run_mode.lower() == "create-or-alter":
                        if sf_state == {}:
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    action="CREATE",
                                    )

                            print(f"\n{GREEN} + Create {sf_object}{RESET}")
                            print(sql)

                            if not dry_run:
                                sf_cursor.execute(sql)

                        # If the object exists, but the definition has a new name, alter name
                        elif sf_state["name"] != d_state["name"]:

                            alter_definition = utils.state_comparison(new_state=d_state, old_state=sf_state)

                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    action="ALTER",
                                    new_name=alter_definition["name"],
                                    old_name=sf_state["name"],
                                    )

                            print(f"\n{YELLOW} ~ Alter {sf_object}{RESET}")
                            print(sql)

                            if not dry_run:
                                sf_cursor.execute(sql)

                        # If the object exists and has the same name, alter the properties of the object
                        elif sf_state["name"] == d_state["name"]:

                            alter_definition = utils.state_comparison(new_state=d_state, old_state=sf_state)

                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    action="ALTER",
                                    )

                            print(f"\n{YELLOW} ~ Alter {sf_object}{RESET}")
                            print(sql)

                            if not dry_run:
                                sf_cursor.execute(sql)

                    elif run_mode.lower() == "destroy":
                        sql = utils.render_templates(
                                template_file=f"{object}.sql",
                                definition=d_state,
                                action="DROP",
                                )

                        print(f"\n{RED} - Drop {sf_object}{RESET}")
                        print(sql)

                        if not dry_run:
                            sf_cursor.execute(sql)

        except Exception as e:
            sf_cursor.close()
            raise TemplateFileError(object, folder=resources_folder, error=e)

    sf_cursor.close()

if __name__ == "__main__":
    main()