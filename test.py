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

    def local_run(self):
        """Load environemnt variables from .env file and full run."""
        load_dotenv()
        main()

    def test_resources(self, resources:list):
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

            # Filter working on resources
            if object in resources:

                file_path = os.path.join("definitions", f"{object}.yml")

                try:
                    with open(file_path) as f:
                        definition = yaml.safe_load(f)
                except FileNotFoundError:
                    raise DefinitionKeyError(object) 

                try:
                    for d_state in definition[object]:
                        if d_state["name"] == object_name:

                            ic("CREATE OR ALTER iac_action")
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="CREATE OR ALTER",
                                    )

                            print(sql)

                            ic("CREATE iac_action")
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="CREATE",
                                    )

                            print(sql)

                            ic("ALTER iac_action where the name of the object differs.")
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="ALTER",
                                    new_name=d_state["name"],
                                    old_name="old_object_name",
                                    )

                            print(sql)

                            ic("ALTER iac_action")
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="ALTER",
                                    )

                            print(sql)

                            ic("DROP iac_action")
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="DROP",
                                    )

                            print(sql)

                            ic("GRANT iac_action")
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="GRANT",
                                    )

                            print(sql)

                            ic("REVOKE iac_action")
                            sql = utils.render_templates(
                                    template_file=f"{object}.sql",
                                    definition=d_state,
                                    iac_action="REVOKE",
                                    )

                            print(sql)

                except Exception as e:
                    raise TemplateFileError(object, folder="resources", error=e)



if __name__ == "__main__":
    local_test = LocalTest()
    resources = ["schema"]
    local_test.test_resources(resources)