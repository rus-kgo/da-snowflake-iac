"""State comparison module.

This module performs the checks of sql database resources drift.
Drift is the term for when the real-world state of your infrastructure differs from the state defined in your configuration.
"""

from __future__ import annotations

import json
from typing import Any, TYPE_CHECKING
from dataclasses import dataclass, field
from errors import SQLExecutionError, DefinitionKeyError
from collections.abc import MutableMapping, MutableSequence

if TYPE_CHECKING:
    from sqlalchemy import Connection

@dataclass
class CheckResult:
    """Result of a drift check.

    Attributes:
        match: True if the definition matches the observed state.
        diff: Details of differences when match is False, or None if no differences.
    """
    match: bool
    diff: dict | set = field(default=None)

class Drift:
    """Drift check of the database resource."""

    def __init__(self, conn:Connection) -> dict:
        """Initialize the comparator with Snowflake connection parameters and YAML definitions file path.

        Args:
            conn(Connection): SQL database connection.
        """
        self.conn = conn

    def __clean_value(self, value:Any) -> Any:
        """Clean and normalize a value."""
        # Handle None
        if value is None:
            return value

        # If already a boolean, keep it
        if isinstance(value, bool):
            return value

        # If it's a string
        if isinstance(value, str):
            string_value = value.upper().strip()

            # Check for boolean strings
            if string_value == "TRUE":
                return True
            if string_value == "FALSE":
                return False

            try:
                # Try int first
                value = int(string_value)
            except ValueError:
                try:
                    # Try float
                    value = float(string_value)
                except ValueError:
                    # Not a number, return string
                    return string_value

        # Default: return as-is
        return value

    def _normalize_definition(self, definition:dict) -> dict:
        """Prepare the defined resource for comparison."""
        # Remove pipeline specific keys
        rcs_def = {
            k:v for k,v in definition.items()
            if k not in {"depends_on", "wait_time"}
            }

        clean_rcs_def = {}
        for key, value in rcs_def.items():

            if isinstance(value, list):
                if all(isinstance(item, str) for item in value):
                    clean_value = [self.__clean_value(i) for i in value]

                elif all(isinstance(item, dict) for item in value):
                    clean_value = [
                        {k.lower().strip():self.__clean_value(v) for k,v in d.items()}
                        for d in value
                    ]
            else:
                clean_value = self.__clean_value(value)

            clean_rcs_def[key.lower().strip()] = clean_value

        return clean_rcs_def

    def _fetch_state_query(self, query:str) -> dict:
        """Fetch the resource state query as a dictionary."""
        try:
            result = self.conn.exec_driver_sql(query).scalar_one_or_none()
            if result:
                return json.loads(result)
        except Exception as err:
            raise SQLExecutionError(error=err) from err
        else:
            return None


    def __flatten_dict_gen(self, d:MutableMapping, parent_key, sep):
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, MutableMapping):
                yield from self._flatten_dict(v, new_key, sep=sep).items()
            elif isinstance(v, MutableSequence):
                for i in v:
                    if isinstance(i,MutableMapping):
                        yield from self._flatten_dict(i, new_key, sep=sep).items()
            else:
                yield new_key, v

    def _flatten_dict(self, d:MutableMapping, parent_key: str = "", sep: str = "::"):
        return dict(self.__flatten_dict_gen(d, parent_key, sep))


    def _check_keys(self, definition:dict, state:dict, name:str) -> CheckResult:
        """Compare the definition keys."""
        definition_keys = set(self._flatten_dict(d=definition).keys())
        state_keys = set(self._flatten_dict(d=state).keys())

        # Symmetric difference (elements in A or B but not both)
        symetric_diff = sorted(definition_keys ^ state_keys)

        if symetric_diff:
            raise DefinitionKeyError(keys=symetric_diff, name=name)

        return CheckResult(match=True)


    def _check_values(self, state:dict, definition:dict) -> CheckResult:

        if definition == state:
            return CheckResult(match=True)

        result = {}
        # Iterate through definition keys (what we expect)
        for key, defn_value in definition.items():
            state_value = state.get(key)

            # Handle dictionaries recursively
            if isinstance(defn_value, dict) and isinstance(state_value, dict):
                nested_result = self._check_values(state_value, defn_value).diff
                if nested_result:  # Only include if there are mismatches
                    result[key] = nested_result

            # Handle lists recursively
            elif isinstance(defn_value, list) and isinstance(state_value, list):
                list_result = []
                # Compare items
                for i in defn_value:
                    if i not in state_value:
                        list_result.append(i)
                    if list_result:
                        result[key] = list_result

            # Handle primitive values
            elif defn_value != state_value:
                # Only include if definition value is not empty
                if defn_value != "" and defn_value is not None:
                    result[key] = defn_value

        return CheckResult(match=False, diff=result)


    def resource_state(
            self,
            definition:dict,
            state_query:str,
            name:str,
            ) -> dict:
        """Compare the resource definition with the resource state."""
        rsc_def = self._normalize_definition(definition)

        rsc_state = self._fetch_state_query(state_query)

        # If the resource does not exists in the database
        if not rsc_state:
            return {
                "iac_action":"create",
                "definition":rsc_def,
            }

        # If the resource exists and the definition keys match the state keys
        rsc_state = self._normalize_definition(rsc_state)
        keys_check = self._check_keys(
            definition=rsc_def,
            state=rsc_state,
            name=name,
            )

        if keys_check.match:
            # Check the value difference
            values_check:CheckResult = self._check_values(
                definition=rsc_def,
                state=rsc_state,
                name=name,
                )

            if not values_check.match:
                return {
                    "iac_action":"alter",
                    "definition":values_check.diff,
                }

        return {
            "iac_action":"no-action",
            "definition":None,
        }

