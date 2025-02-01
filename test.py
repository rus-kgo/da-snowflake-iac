"""Locar run test module.

This module loads the enironemnt variables from .env file an runs the project from main().
The .env file should have the following variables.

GITHUB_WORKSPACE = ""
INPUT_DEFINITIONS-PATH= "definitions"
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
from dotenv import load_dotenv
from main import main


load_dotenv()

if __name__ == "__main__":
    main()