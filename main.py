"""Main entrance point of the pipeline.

This module provides:
- main: main function that orchestrates the pipeline;
- str_to_bool: function for bool input vars;
- str_to_json: function for json string input vars;
- to_str: function for string input vars that might be empty or null;
"""

import yaml 
import os
import re
import json
import snowflake.connector as sc

from utils import Utils
from errors import DefinitionKeyError, TemplateFileError, SnowflakeConnectionError
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

def str_to_json(s: str | None) -> dict | None:
    """Convert a string it a json dict."""
    if s is None or s == "" or s == "None":
        return None

    return json.loads(s)

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
        definitions_path = os.environ.get("INPUT_DEFINITIONS-PATH")
        resources_path = os.environ.get("INPUT_RESOURCES-PATH")
        dry_run = str_to_bool(os.environ["INPUT_DRY-RUN"])
        run_mode = os.environ["INPUT_RUN-MODE"]
        database = to_str(os.environ.get("INPUT_DATABASE", None)) 
        schema = to_str(os.environ.get("INPUT_SCHEMA", None)) 
        warehouse_size = to_str(os.environ.get("INPUT_WAREHOUSE-SIZE", None)) 

        # Environment
        user = os.environ["SNOWFLAKE_USER"]
        account = os.environ["SNOWFLAKE_ACCOUNT"]
        warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
        sf_app_client_id = os.environ["SNOWFLAKE_APP_CLIENT_ID"]
        sf_app_client_secret = os.environ["SNOWFLAKE_APP_CLIENT_SECRET"]
        sf_app_tenant_id = os.environ["SNOWFLAKE_APP_TENANT_ID"]
        sf_app_scope = os.environ["SNOWFLAKE_APP_SCOPE"]
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e  # noqa: TRY003


    # Snowflake resources that contain definition in the SHOW command.
    show_only_objects = ["view","database", "role"]

    # Snowflake resource with DESCRIBE output as nested field.
    nested_desc = [{"dynamic table":"columns"}]

    # Snowflake resources that can be granted or revokeced
    granting = ["grant"]

    # Object with create or alter new feature in preview that combines create and alter in on DDL
    # https://docs.snowflake.com/en/sql-reference/sql/create-or-alter
    create_or_alter = ["views","table","procedure", "column","schema","task"]

    resources_folder = resources_path
    definitions_path = f"{workspace}{definitions_path}"

    utils = Utils(
        sf_app_client_id = sf_app_client_id,
        sf_app_client_secret = sf_app_client_secret,
        sf_app_tenant_id = sf_app_tenant_id,
        sf_app_scope = sf_app_scope,
        definitions_folder=definitions_path,
        resources_folder=resources_folder,
    )

    # First check if all definitions have their tags, assign a tag if not
    utils.assign_pipeline_tag_id()

    # Map the dependencies of all the definitions in the yaml files
    map = utils.dependencies_map()

    # Do topographic sorting of the dependecies
    sorted_map = utils.dependencies_sort(map)["map"]

    # Get toke for Snowflake connection
    token = utils.get_oauth_access_token()

    try:
        conn_params = {
            "user":user,
            "account":account,
            "authenticator":"oauth",
            "token":token,
            "warehouse":warehouse,
            "database":database,
            "schema":schema,
        }

        conn = sc.connect(**conn_params)

        if warehouse_size:
            cur = conn.cursor()
            # Get the current warehouse size to set it back to default at the and of the run.
            cur.execute(f"SHOW PARAMETERS LIKE 'warehouse_size' IN WAREHOUSE {warehouse};")
            default_warehouse_size = cur.fetchone()[1]

            # Set the warehouse size for the run
            cur.execute(f"alter warehouse {warehouse} set warehouse_size = {warehouse_size};")

    except Exception as e:
        raise SnowflakeConnectionError(error=e, conn_params=conn_params)




    # Initate drift class to compare object states
    drift = ObjectDriftCheck(conn=conn)

    # Print out the map planning, excecute if not a dry-run.
    print(f"{CYAN}account: {conn_params['account']}{RESET}\n")
    for i in sorted_map:
        object, object_name = i.split("::")

        file_path = os.path.join(definitions_path, f"{object}.yml")

        try:
            with open(file_path) as f:
                definition = yaml.safe_load(f)
        except FileNotFoundError:
            raise DefinitionKeyError(object) 

        # Making sure the object is correctly named `database role` instead of `database_role`
        sf_object = re.sub(r"_", " ", object)


        # Granting snowflake objects that are not created or dropped
        if sf_object in granting:
            try:
                for d_state in definition[object]:
                    # This is for benefit of following the sorter order
                    if d_state["name"] == object_name and run_mode.lower() == "create-or-update":
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="GRANT",
                                    )
                            print(f"\n{GREEN} + Grant {sf_object}{RESET}")
                            print(sql)

                            if not dry_run:
                                utils.execute_rendered_template(connection=conn, definition=d_state, sql=sql)

                    if d_state["name"] == object_name and run_mode.lower() == "destroy":
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="REVOKE",
                                    )
                            print(f"\n{RED} - Revoke {sf_object}{RESET}")
                            print(sql)

                            if not dry_run:
                                utils.execute_rendered_template(connection=conn, definition=d_state, sql=sql)
            except Exception as e:
                if warehouse_size:
                    cur.execute(f"alter warehouse {warehouse} set warehouse_size = {default_warehouse_size};")
                conn.close()
                raise TemplateFileError(object, folder=resources_folder, error=e)
        
        # Create or alter new feature in preview that combines create and alter in one DDL
        # https://docs.snowflake.com/en/sql-reference/sql/create-or-alter
        elif sf_object in create_or_alter:
            try:
                for d_state in definition[object]:
                    # This is for benefit of following the sorter order
                    if d_state["name"] == object_name and run_mode.lower() == "create-or-update":
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="CREATE OR ALTER",
                                    )
                            print(f"\n{BRIGHT_GREEN} +~ Create or Alter {sf_object}{RESET}")
                            print(sql)

                            if not dry_run:
                                utils.execute_rendered_template(connection=conn, definition=d_state, sql=sql)

                    if d_state["name"] == object_name and run_mode.lower() == "destroy":
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="DROP",
                                    )
                            print(f"\n{RED} - Drop {sf_object}{RESET}")
                            print(sql)

                            if not dry_run:
                                utils.execute_rendered_template(connection=conn, definition=d_state, sql=sql)
            except Exception as e:
                if warehouse_size:
                    cur.execute(f"alter warehouse {warehouse} set warehouse_size = {default_warehouse_size};")
                conn.close()
                raise TemplateFileError(object, folder=resources_folder, error=e)


        else:
            
            try:
                for d_state in definition[object]:
                    # This is for benefit of following the sorter order
                    if d_state["name"] == object_name:

                        # Here we are getting the tag to compare it with the snowfalke object instead o using the name
                        object_id_tag = d_state["object_id_tag"]

                        describe_object = "No" if sf_object in show_only_objects else "Yes"

                        for d in nested_desc:
                            nested_field = d.get(sf_object, None)


                        sf_drift = drift.object_state(
                            object_id=object_id_tag, 
                            object_definition=d_state, 
                            sf_object=sf_object,
                            describe_object=describe_object,
                            nested_field=nested_field,
                            )

                        if run_mode.lower() == "create-or-update":
                            if not sf_drift:
                                sql = utils.render_templates(
                                        template_file=f"{object}.sql",
                                        definition=d_state,
                                        iac_action="CREATE",
                                        )

                                print(f"\n{GREEN} + Create {sf_object}{RESET}")
                                print(sql)

                                if not dry_run:
                                    utils.execute_rendered_template(connection=conn, definition=d_state, sql=sql)

                            # Do nothing if the states are the same 
                            elif len(sf_drift) == 0:
                                continue

                            # If the object exists, but the definition has a new name, alter name
                            elif sf_drift["name"] != d_state["name"]:

                                sql = utils.render_templates(
                                        template_file=f"{object}.sql",
                                        definition=d_state,
                                        iac_action="ALTER",
                                        new_name=d_state["name"],
                                        old_name=sf_drift["name"],
                                        )

                                print(f"\n{YELLOW} ~ Alter {sf_object}{RESET}")
                                print(sql)

                                if not dry_run:
                                    utils.execute_rendered_template(connection=conn, definition=d_state, sql=sql)

                            # If the object exists and has the same name, alter the properties of the object
                            elif sf_drift["name"] == d_state["name"]:

                                sql = utils.render_templates(
                                        template_file=f"{object}.sql",
                                        definition=d_state,
                                        iac_action="ALTER",
                                        )

                                print(f"\n{YELLOW} ~ Alter {sf_object}{RESET}")
                                print(sql)

                                if not dry_run:
                                    utils.execute_rendered_template(connection=conn, definition=d_state, sql=sql)

                        elif run_mode.lower() == "destroy":
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="DROP",
                                    )

                            print(f"\n{RED} - Drop {sf_object}{RESET}")
                            print(sql)

                            if not dry_run:
                                    utils.execute_rendered_template(connection=conn, definition=d_state, sql=sql)

            except Exception as e:
                if warehouse_size:
                    cur.execute(f"alter warehouse {warehouse} set warehouse_size = {default_warehouse_size};")
                conn.close()
                raise TemplateFileError(object, folder=resources_folder, error=e)

    if warehouse_size:
        cur.execute(f"alter warehouse {warehouse} set warehouse_size = {default_warehouse_size};")
    conn.close()

if __name__ == "__main__":
    main()