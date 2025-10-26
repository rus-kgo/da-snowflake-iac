"""Main entrance point of the pipeline.

This module provides:
- main: main function that orchestrates the pipeline;
- str_to_bool: function for bool input vars;
- to_str: function for string input vars that might be empty or null;
"""

import yaml 
import os
import re
import json
import tomllib

from utils import Utils
from errors import DefinitionKeyError, TemplateFileError, FilePathError
from drift import ObjectDriftCheck

RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"
BRIGHT_GREEN = "\033[92m"
YELLOW = "\033[33m"
CYAN = "\033[36m"


def str_to_bool(s: str) -> bool:
    """Convert input string to a boolean."""
    s = s.lower()
    if s not in {"true", "false"}:
        raise ValueError(f"Invalid value for boolean: {s}")  # noqa: TRY003
    return s == "true"

def to_str(s: str | None) -> str | None:
    """Make sure it a string, return None empty."""
    if s is None or s == "None" or s == "":
        return None
    return s


def main():
    """Entry point of the pipeline."""
    try:
        workspace = os.environ["GITHUB_WORKSPACE"]

        # Inputs
        database_system = os.environ["INPUT_DATABASE-SYSTEM"]
        definitions_path = os.environ.get("INPUT_DEFINITIONS-PATH")
        resources_path = os.environ.get("INPUT_RESOURCES-PATH")
        dry_run = str_to_bool(os.environ["INPUT_DRY-RUN"])
        run_mode = os.environ["INPUT_RUN-MODE"]

    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e  # noqa: TRY003


    definitions_path = f"{workspace}{definitions_path}"

    utils = Utils(
        definitions_path=definitions_path,
        resources_path=resources_path,
    )

    # Map the dependencies of all the definitions in the yaml files
    map = utils.dependencies_map()

    # Do topographic sorting of the dependecies
    sorted_map = utils.dependencies_sort(map)["map"]

    # Establish the connection
    conn = utils.create_db_sys_connection(database_system=database_system)

    # Initate drift class to compare object states
    drift = ObjectDriftCheck(conn=conn)

    # Load all resources
    try:
        with open(resources_path, "rb") as f:
            db_sys_config = tomllib.load(f)
            db_sys_resources = db_sys_config[database_system]["resources"]
    except FileNotFoundError:
        raise FilePathError(resources_path)


    # Print out the map planning, excecute if not a dry-run.
    for i in sorted_map:
        object, object_name = i.split("::")

        file_path = os.path.join(definitions_path, f"{object}.toml")

        try:
            with open(file_path, "rb") as f:
                definition = tomllib.load(f)
        except FileNotFoundError:
            raise DefinitionKeyError(object) 

        try:
            for obj_def in definition[object]:
                # This is for benefit of following the sorter order
                if obj_def["name"] == object_name:

                    # Check if the object defintion has drifted from it's state in the database.
                    obj_drift = drift.object_state(object_definition=obj_def)

                    if run_mode.lower() == "create-or-update":
                        # If there is no drift, then it is a new object.
                        if not obj_drift:
                            template = db_sys_resources[object]["template"]
                            iac_action = db_sys_resources[object]["iac_action"]
                            sql = utils.render_templates(
                                    template=template,
                                    definition=obj_def,
                                    obj_name=object_name,
                                    iac_action=iac_action["create"],
                                    )

                            print(f"\n{GREEN} + Create '{object}'{RESET}")
                            print(sql)

                            if not dry_run:
                                print(utils.execute_rendered_sql_template(
                                    connection=conn,
                                    object=object,
                                    object_name=object_name,
                                    sql=sql,
                                ))

                        # Do nothing if the the object has not drifted, definition and the state are the same.
                        elif len(obj_drift) == 0:
                            continue


                        # If the object drifted, alter the properties of the object.
                        elif obj_drift["name"] == obj_def["name"]:

                            sql = utils.render_templates(
                                    template=template,
                                    definition=obj_def,
                                    obj_name=object_name,
                                    iac_action=iac_action["alter"],
                                    )

                            print(f"\n{YELLOW} ~ Alter '{object}'{RESET}")
                            print(sql)

                            if not dry_run:
                                utils.execute_rendered_sql_template(connection=conn, definition=obj_def, sql=sql)

                    elif run_mode.lower() == "destroy":
                        sql = utils.render_templates(
                                template=template,
                                definition=obj_def,
                                obj_name=object_name,
                                iac_action=iac_action["drop"],
                                )

                        print(f"\n{RED} - Drop '{object}'{RESET}")
                        print(sql)

                        if not dry_run:
                                utils.execute_rendered_sql_template(connection=conn, definition=obj_def, sql=sql)

        except Exception as e:
            conn.close()
            raise TemplateFileError(object, folder=resources_path, error=e)
    conn.close()

if __name__ == "__main__":
    main()