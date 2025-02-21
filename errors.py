"""Error classes for the pipeline.

This module provides:
- FilePathError: exception when the file path is incorrect;
- DefinitionKeyError: exception when the definition yaml file keys are incorrect;
- DependencyError: exception when the names of the objects in the dependecy map are incorrect.
"""

import json

class FilePathError(Exception):
    """File path error class."""

    def __init__(self, *args):
        """Combine all invalid paths into a single string."""
        paths = "', '".join(args) 
        super().__init__(f"One of the file paths: '{paths}' - is not valid.")
        
class TemplateFileError(Exception):
    """Raised when there is an issue with the resources template file."""

    def __init__(self, file, folder, error):
        """Define the message.

        Args:
            error (list): Jinja2 error details.
            file (str): The name of the file containing the invalid definition.
            folder (str): The name of the folder containing the invalid file.

        """
        message = f"Invalid or missing template file = '{file}.sql' in the resouces folder = '{folder}'.\nJinja2 error: {error}"

        super().__init__(message)

class DefinitionKeyError(Exception):
    """Raised when there are missing or invalid keys in a definition file."""

    def __init__(self, *keys, file=None, obj_name=None):
        """Define the message.

        Args:
            keys (list): The invalid or missing keys.
            file (str, optional): The name of the file containing the invalid definition.
            obj_name (str, optional): The specific object name where the keys are missing or invalid.

        """
        keys_str = "', '".join(keys)

        if file and obj_name:
            message = f"Invalid or missing keys: ['{keys_str}'] for the object name = '{obj_name}' in the file = '{file}.yml'."
        elif file:
            message = f"Invalid of missing keys: ['{keys_str}'] in the file = '{file}.yml'."
        else:
            message = (
                f"One of the definitions yaml files has an invalid or missing keys: '{keys_str}'."
            )
        super().__init__(message)

class DependencyError(Exception):
    """Dependencies names errors."""

    def __init__(self, map:dict):
        """Take the dependency map as a variable and format it for readability."""
        formatted_map = json.dumps(map, indent=4)
        super().__init__(f"There is an incorrect dependency in the map:\n{formatted_map}\nMake sure the objects names are correct.")

class SnowflakeConnectionError(Exception):
    """Raised when there is an issue establishing a Snowflake connection or cursor."""

    def __init__(self, error, conn_params=None):
        """Define the error message.
        
        Args:
            error (Exception): The original exception raised by the Snowflake connector.
            conn_params (dict, optional): Connection parameters (excluding sensitive data like private keys).
            
        """
        sanitized_params = {
            key: value
            for key, value in (conn_params or {}).items()
            if key not in ("private_key", "private_key_pwd")
        }
        
        message = (
            f"An error occurred while trying to establish a connection or create a cursor in Snowflake.\n"
            f"Connection parameters: {sanitized_params}\n"
            f"Original error: {error}"
        )
        
        super().__init__(message)

class ClientCredentialsError(Exception):
    """Rised when there is an issue getting a secret from AWS."""

    def __init__(self, error:str):
        """Define error message."""
        message = (
            f"An error occurred while trying to get a secret from AWS Secret Manager.\n"
            f"Original error: {error}"
        )
        super().__init__(message)

class OAuthTokenError(Exception):
    """Custom exception for errors occurring during token request."""

    def __init__(self,response=None, error=None):
        """Define error mssage.

        Args:
            response (requests.Response, optional): HTTP response object (if available).
            error (Exception, optional): Original exception that was caught (if any).

        """
        super().__init__()
        self.response = response
        self.error = error
        self.status_code = response.status_code if response else None
        self.content = response.raise_for_status() if response else None

    def __str__(self):
        """Return a string representation of the message error."""
        base_message = ("An error occurred while trying to get the access token for Snowflake API.\n")
        if self.original_exception:
            base_message += f"Original error: {self.error}"
        if self.status_code:
            base_message += f"Status Code: {self.status_code})"
        if self.content:
            base_message += f"\nReason: {self.content}"
        return base_message
