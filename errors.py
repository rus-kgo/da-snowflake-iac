"""Error classes for the pipeline.

This module provides:
- FilePathError: exception when the file path is incorrect;
- DefinitionKeyError: exception when the definition yaml file keys are incorrect;
- DependencyError: exception when the names of the resources in the dependecy map are incorrect.
"""

import json
from rich.console import Console
from rich.syntax import Syntax

# Maximum number of characters to include in SQL preview in error messages
SQL_PREVIEW_MAX_LENGTH = 500


class FilePathError(Exception):
    """File path error class."""

    def __init__(self, path:str=None, resource_type:str=None):
        """Initialize the FilePathError with an optional path and resource type.

        Args:
            path (str, optional): The file path that caused the error.
            resource_type (str, optional): The type of resource related to the file path.
        """
        if path:
            message = f"Not a valid file path: '{path}'"
        elif path and resource_type:
            message = f"Not a valid file path: '{path}' for " \
                f"resource: '{resource_type}'"
        super().__init__(message)

class TemplateFileError(Exception):
    """Raised when there is an issue with the resources template file."""

    def __init__(self, name: str, file: str, error: Exception):
        """Initialize the exception with detailed error information.

        Args:
            file (str): The name of the template file (without extension).
            error (Exception): The caught Jinja2 exception instance.
            name (str): The name of the resource.

        """
        if hasattr(error, "lineno"):
            message = (
                f"Template syntax error of the resource: '{name}' at line {error.lineno} "
                f"in file = '{file}'\n"
                f"Jinja2 error: {str(error)}"
            )
        elif hasattr(error, "message"):
            message = (
                f"Template rendering error of the resource: '{name}' in file '{file}': {error.message}"
            )
        else:
            message = (
                f"Invalid or missing template for the resource: '{name}' in file = '{file}'.\n"
                f"Jinja2 error: {str(error)}"
            )

        super().__init__(message)

class DefinitionKeyError(Exception):
    """Raised when there are missing or invalid keys in a definition file."""

    def __init__(self, keys:list, name:str=None, file:str=None):
        """Define the message.

        Args:
            keys (list): The invalid or missing keys.
            file (str, optional): The name of the file containing the invalid definition.
            name (str, optional): The specific resource name where the keys are missing or invalid.

        """
        keys_str = "\n- ".join(keys)

        if name:
            message= f"Invalid or missing definition variables for the resource: '{name}'." \
                 "\nVariables: " \
                f"\n- {keys_str}"

        elif file and name:
            message = f"Invalid or missing definition variables for the resource: '{name}'." \
                f"\nDefinition file: '{file}'." \
                 "\nVariables: " \
                f"\n- {keys_str}"
        else:
            message = "One of the definitions files has an invalid or missing variables:" \
                f"\n- {keys_str}"

        super().__init__(message)

class DependencyError(Exception):
    """Dependencies names errors."""

    def __init__(self, d_map:dict,*,is_cyclical:bool = False):
        """Take the dependency map as a variable and format it for readability."""
        formatted_map = json.dumps(d_map, indent=4)
        if is_cyclical:
            message = f"There is a cyclical dependecy in the map: \n{formatted_map}."
        else:
            message = f"There is an incorrect dependency in the map:\n{formatted_map}" \
            "\nMake sure the resources names are correct."

        super().__init__(message)

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
        database_system: str = None,
    ):
        """Initialize the exception with detailed error information.

        Args:
            error (Exception): The original SQLAlchemy or database exception.
            sql (str, optional): The SQL statement that failed.
            resource_type (str, optional): Type of database resource (table, view, role, etc.).
            resource_name (str, optional): Name of the resource being operated on.
            action (str, optional): The action being performed (created, dropped, altered, etc.).
            database_system (str, optional): The database system (snowflake, sqlite, etc.).

        """
        # Extract error details from SQLAlchemy exception
        error_message = str(error)
        error_code = getattr(error, "code", None)
        orig_error = getattr(error, "orig", None)

        # Build the colored error message for GitHub Actions
        parts = ["[bold red3]SQL EXECUTION ERROR[/bold red3]"]

        # Add database system if available
        if database_system:
            parts.append(f"Database System: [bold cyan3]{database_system.upper()}[/bold cyan3]")

        # Add error code if available
        if error_code:
            parts.append(f"[bold red3]Error Code: {error_code}[/bold red3]")

        # Add the original error message
        parts.append("\n[bold red3]Error Message:[/bold red3]")
        parts.append(f"[bold red3]{error_message}[/bold red3]")

        # Add original database error if different from SQLAlchemy wrapper
        if orig_error and str(orig_error) != error_message:
            parts.append("\n[bold red3]Database Error:[/bold red3]")
        if sql:
            sql_preview = sql if len(sql) <= SQL_PREVIEW_MAX_LENGTH else sql[:SQL_PREVIEW_MAX_LENGTH] + "..."
            pretty_sql = Syntax(sql_preview, "sql", theme="monokai", line_numbers=False)
            parts.append("\n[bold red3]SQL Statement:[/bold red3]")
            parts.append(pretty_sql)

        # Join all parts into final message
        message = "\n".join(parts)

        Console().print(message)

        super().__init__("\nEnd.")

        # Store attributes for programmatic access
        self.original_error = error
        self.sql = sql
        self.database_system = database_system
        self.error_code = error_code
