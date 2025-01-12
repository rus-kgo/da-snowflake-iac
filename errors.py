"""Error classes for the pipeline.

This module provides:
- FilePathError: exception when the file path is incorrect;
- DefinitionKeyError: exception when the definition yaml file keys are incorrect;
- DependencyError: exception when the names of the objects in the dependecy map are incorrect.
"""

import json

class FilePathError(Exception):
    """File path error class."""

    def __init__(self, *args, cwd):
        """Combine all invalid paths into a single string."""
        paths = "', '".join(args) 
        super().__init__(f"One of the file paths: '{paths}' - is not valid for the current working directory: '{cwd}'.")


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
            message = f"Invalid or missing keys: '{keys_str} for the object name = '{obj_name}' in the file = '{file}.yml'."
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