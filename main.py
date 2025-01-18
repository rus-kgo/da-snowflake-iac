"""Main entrance point of the pipeline.

This module provides:
- main: main function that orchestrates the pipeline;
"""

import yaml 
import os
import re
import json
import snowflake.connector as sc

from utils import Utils
from errors import DefinitionKeyError, TemplateFileError, SnowflakeConnectionError

RESET = "\033[0m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
CYAN = "\033[36m"


def str_to_bool(s: str) -> bool:
    """Convert input string to a boolean."""
    s = s.lower()
    if s not in {"true", "false"}:
        raise ValueError(f"Invalid value for boolean: {s}")
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
        # Inputs
        definitions_path = os.environ.get("INPUT_DEFINITIONS-PATH")
        dry_run = str_to_bool(os.environ["INPUT_DRY-RUN"])
        run_mode = os.environ["INPUT_RUN-MODE"]
        vars = str_to_json(os.environ.get("INPUT_VARS", None))
        database = to_str(os.environ.get("INPUT_DATABASE", None)) 
        schema = to_str(os.environ.get("INPUT_SCHEMA", None)) 

        # Environment
        user = os.environ["SNOWFLAKE_USER"]
        account = os.environ["SNOWFLAKE_ACCOUNT"]
        warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
        sf_conn_secret_name = os.environ["SNOWFLAKE_CONN_SECRET_NAME"]
        aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
        aws_region_name = os.environ["AWS_REGION"]
        aws_role_arn = os.environ["AWS_ROLE_ARN"]
    except KeyError as e:
        raise ValueError(f"Missing environment variable: {e}") from e


    # definitions_folder = "/github/workspace/definitions"
    resources_folder = "/snowflake-iac/resources"

    definitions_path = os.path.join(os.getcwd(), definitions_path)

    utils = Utils(
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        aws_region_name = aws_region_name,
        aws_role_arn = aws_role_arn,
        definitions_folder=definitions_path,
        resources_folder=resources_folder,
        snowflake_client_secret=sf_conn_secret_name,
    )

    try:
        token = utils.get_oauth_access_token()

        conn_params = {
            "user":user,
            "account":account,
            "authenticator":"oauth",
            "token":token,
            "warehouse":warehouse,
            "database":database,
            "schema":schema,
        }

        ctx = sc.connect(**conn_params)

        sf_cursor = "" #ctx.cursor()

    except Exception as e:
        raise SnowflakeConnectionError(error=e, conn_params=conn_params)



    # First check if all definitions have their tags, assign a tag if not
    utils.assign_pipeline_tag_id()

    # Map the dependencies of all the definitions in the yaml files
    map = utils.dependencies_map()

    # Do topographic sorting of the dependecies
    sorted_map = utils.dependencies_sort(map)["map"]


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