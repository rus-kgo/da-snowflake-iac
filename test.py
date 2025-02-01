"""Locar run test module.

This module loads the enironemnt variables from .env file an runs the project from main().
The .env file should have the following variables.

GITHUB_WORKSPACE = ""
INPUT_DEFINITIONS-PATH = "definitions"
INPUT_RESOURCES-PATH = "resources"
INPUT_DRY-RUN = "True"
INPUT_RUN-MODE= "create-or-alter"
INPUT_DATABASE= "some_database"
INPUT_SCHEMA= "some_schema"
SNOWFLAKE_USER= "some_user"
SNOWFLAKE_ACCOUNT= "some_account"
SNOWFLAKE_WAREHOUSE= "some_warehouse"
SNOWFLAKE_CONN_SECRET_NAME= "some_secret_name"
AWS_ACCESS_KEY_ID= "some_aws_access_key"
AWS_SECRET_ACCESS_KEY= "some_aws_secret_key"
AWS_REGION= "some_aws_region"
AWS_ROLE_ARN= "some_aws_role_arn"

"""
import os
import yaml
from dotenv import load_dotenv
from icecream import ic

from errors import DefinitionKeyError, TemplateFileError
from main import main
from utils import Utils

class LocalTest:
    """Local test class to test the resources and full local run."""

    def local_run():
        """Load environemnt variables from .env file and full run."""
        load_dotenv()
        main()

    def test_resources():
        """Load resources and definitions and print out the output for testgin."""
        utils = Utils(
            resources_folder="resources",
            definitions_folder="definitions",
        )

        # First check if all definitions have their tags, assign a tag if not
        utils.assign_pipeline_tag_id()

        # Map the dependencies of all the definitions in the yaml files
        map = utils.dependencies_map()
        ic(map)

        # Do topographic sorting of the dependecies
        sorted_map = utils.dependencies_sort(map)["map"]
        ic(sorted_map)


        for i in sorted_map:
            object, object_name = i.split("::")

            file_path = os.path.join("definitions", f"{object}.yml")

            try:
                with open(file_path) as f:
                    definition = yaml.safe_load(f)
            except FileNotFoundError:
                raise DefinitionKeyError(object) 

            try:
                for d_state in definition[object]:
                    if d_state["name"] == object_name:

                        ic("CREATE action")
                        sql = utils.render_templates(
                                template_file=f"{object}.sql",
                                definition=d_state,
                                action="CREATE",
                                )

                        print(sql)

                        ic("ALTER action where the name of the object differs.")
                        sql = utils.render_templates(
                                template_file=f"{object}.sql",
                                definition=d_state,
                                action="ALTER",
                                new_name=d_state["name"],
                                old_name="old_object_name",
                                )

                        print(sql)

                        ic("ALTER action")
                        sql = utils.render_templates(
                                template_file=f"{object}.sql",
                                definition=d_state,
                                action="ALTER",
                                )

                        print(sql)

                        ic("DROP action")
                        sql = utils.render_templates(
                                template_file=f"{object}.sql",
                                definition=d_state,
                                action="DROP",
                                )

                        print(sql)


            except Exception as e:
                raise TemplateFileError(object, folder="resources", error=e)



if __name__ == "__main__":
    local_test = LocalTest
    local_test.test_resources()