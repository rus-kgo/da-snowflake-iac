"""Error classes for the pipeline.

This module provides:
- FilePathError: exception when the file path is incorrect;
- DefinitionKeyError: exception when the definition yaml file keys are incorrect;
- DependencyError: exception when the names of the objects in the dependecy map are incorrect.
"""

import json

# ANSI color codes for terminal output
RESET = "\033[0m"
RED = "\033[31m"
YELLOW = "\033[33m"
CYAN = "\033[36m"

class FilePathError(Exception):
    """File path error class."""

    def __init__(self, *args):
        """Combine all invalid paths into a single string."""
        paths = "', '".join(args) 
        super().__init__(f"One of the file paths: '{paths}' - is not valid.")
        
class TemplateFileError(Exception):
    """Raised when there is an issue with the resources template file."""

    def __init__(self, obj_name: str, file: str, error: Exception):
        """Initialize the exception with detailed error information.

        Args:
            file (str): The name of the template file (without extension).
            error (Exception): The caught Jinja2 exception instance.
            obj_name (str): The name of the resource object.

        """
        if hasattr(error, "lineno"): 
            message = (
                f"Template syntax error of the object named as '{obj_name}' at line {error.lineno} "
                f"in file = '{file}'\n"
                f"Jinja2 error: {str(error)}"
            )
        elif hasattr(error, "message"):
            message = (
                f"Template rendering error of the object named as '{obj_name}' in file '{file}': {error.message}"
            )
        else:
            message = (
                f"Invalid or missing template for the object named as '{obj_name}' in file = '{file}'.\n"
                f"Jinja2 error: {str(error)}"
            )

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
            if key not in ("private_key", "private_key_pwd", "token", "id_token", "access_token")
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

class SQLExecutionError(Exception):
    """Raised when SQL execution fails in the database."""

    def __init__(
        self, 
        error: Exception, 
        sql: str = None, 
        object_type: str = None, 
        object_name: str = None,
        action: str = None,
        database_system: str = None
    ):
        """Initialize the exception with detailed error information.

        Args:
            error (Exception): The original SQLAlchemy or database exception.
            sql (str, optional): The SQL statement that failed.
            object_type (str, optional): Type of database object (table, view, role, etc.).
            object_name (str, optional): Name of the object being operated on.
            action (str, optional): The action being performed (created, dropped, altered, etc.).
            database_system (str, optional): The database system (snowflake, sqlite, etc.).

        """
        # Extract error details from SQLAlchemy exception
        error_message = str(error)
        error_code = getattr(error, 'code', None)
        orig_error = getattr(error, 'orig', None)
        
        # Build the colored error message for GitHub Actions
        parts = [
            f"\n{RED}{'='*80}",
            f"SQL EXECUTION ERROR",
            f"{'='*80}{RESET}\n"
        ]
        
        # Add object context if available
        if object_type and object_name:
            parts.append(
                f"{RED}Failed to {action or 'execute'} {object_type}: {CYAN}'{object_name}'{RESET}"
            )
        
        # Add database system if available
        if database_system:
            parts.append(f"{RED}Database System: {CYAN}{database_system.upper()}{RESET}")
        
        # Add error code if available
        if error_code:
            parts.append(f"{RED}Error Code: {CYAN}{error_code}{RESET}")
        
        # Add the original error message
        parts.append(f"\n{RED}Error Message:{RESET}")
        parts.append(f"{RED}{error_message}{RESET}")
        
        # Add original database error if different from SQLAlchemy wrapper
        if orig_error and str(orig_error) != error_message:
            parts.append(f"\n{RED}Database Error:{RESET}")
            parts.append(f"{RED}{orig_error}{RESET}")
        
        # Add SQL statement if provided (truncated if too long)
        if sql:
            sql_preview = sql if len(sql) <= 500 else sql[:500] + "..."
            parts.append(f"\n{RED}SQL Statement:{RESET}")
            parts.append(f"{YELLOW}{sql_preview}{RESET}")
        
        parts.append(f"\n{RED}{'='*80}{RESET}\n")
        
        # Join all parts into final message
        message = "\n".join(parts)
        
        super().__init__(message)
        
        # Store attributes for programmatic access
        self.original_error = error
        self.sql = sql
        self.object_type = object_type
        self.object_name = object_name
        self.action = action
        self.database_system = database_system
        self.error_code = error_code