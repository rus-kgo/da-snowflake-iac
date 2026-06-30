"""State comparison module.

This module performs the checks of sql database resources drift.
Drift is the term for when the real-world state of your infrastructure differs from the state defined in your configuration.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pprint import pformat
from typing import TYPE_CHECKING, Any

from dictdiffer import diff

if TYPE_CHECKING:
    from sqliac.adapters.base import BaseAdapter

from sqliac import DDLCommand
from sqliac.constants import Paths
from sqliac.errors import RustyError
from sqliac.value_sanitizer import ValueSanitizer

logger = logging.getLogger(__name__)


@dataclass
class DriftDDLContext:
    """Stores the detected drift between the defined resource and database state.

    The ``add``, ``change``, and ``remove`` sections group differences
    identified during state comparison.

    These names mirror the operation types returned by ``dictdiffer.diff()``:

    Args:
        ddl_comand(DDLCommand): DDL SQL statemens such as CREATE, ALTER, or DROP
        name(str): Provider resource name
        add(dict): values present in the definition but missing in the database state
        change(dict): values that exist in both but differ
        remove(dict): values present in the database state but not in the definition
    """

    ddl_command: DDLCommand
    name: str
    add: dict[str, set[frozenset]] = field(default_factory=dict)
    change: dict[str, set[frozenset]] = field(default_factory=dict)
    remove: dict[str, set[frozenset]] = field(default_factory=dict)

    def __setitem__(self, key: str, value: Any) -> None:  # noqa: D105
        if key not in {"add", "change", "remove"}:
            raise RustyError(
                error=f'invalid key "{key}", expected one of: add, change, remove'
            )

        setattr(self, key, value)

    def __getitem__(self, key: str) -> Any:  # noqa: D105
        if key not in {"add", "change", "remove"}:
            raise RustyError(
                error=f'invalid key "{key}", expected one of: add, change, remove'
            )

        return getattr(self, key)

    def add_frozen(self, action: str, key: str, value: dict) -> None:
        section = getattr(self, action)
        section.setdefault(key, set()).add(frozenset(value.items()))

    def union_frozen(
        self,
        action: str,
        key: str,
        values: set,
    ) -> None:
        section = self[action]
        section.setdefault(key, set()).update(values)

    def _unfreeze_item_or_set(self, item: Any) -> Any:
        """Helper to convert a set of frozensets to a list of dicts, or return item as is."""
        if isinstance(item, set):
            return [dict(frozen_item) for frozen_item in item]
        return item

    def _unfreeze_section(
        self, section: dict[str, set[frozenset]]
    ) -> dict[str, list[dict]]:
        """Converts internal frozen structural sets back to normal list of dicts."""
        return {
            key: self._unfreeze_item_or_set(value) for key, value in section.items()
        }

    def to_dict(self) -> dict[str, Any]:
        """Return data class as a dictionary with frozen sets unfrozen."""
        return {
            "ddl_command": self.ddl_command.value
            if hasattr(self.ddl_command, "value")
            else self.ddl_command,
            "name": self.name,
            "add": self._unfreeze_section(self.add),
            "change": self._unfreeze_section(self.change),
            "remove": self._unfreeze_section(self.remove),
        }


class Drift:
    """Drift check of the database resource."""

    def __init__(self, connection: BaseAdapter):
        """Initialize with dabase connection.

        Args:
            connection(BaseAdapter): SQL database connection.
        """
        self.connection = connection
        self.sanitizer = ValueSanitizer()

    def _normalize_definition(self, definition: dict) -> dict:
        """Prepare the defined resource for comparison."""
        ddl_context: dict[str, Any] = {
            key: value
            for key, value in definition.items()
            if key not in {"depends_on", "wait_time"}
        }

        ddl_context_clean = {
            key: self.sanitizer.deep_clean(value) for key, value in ddl_context.items()
        }

        return ddl_context_clean

    def _normalize_state(self, state: Any, definition: Any) -> Any:
        """Recursively filter state to only include keys present in definition."""
        if isinstance(definition, dict) and isinstance(state, dict):
            return {
                k: self._normalize_state(state[k], v)
                for k, v in definition.items()
                if k in state
            }
        elif isinstance(definition, list) and isinstance(state, list):
            # For lists, we align by index
            return [
                self._normalize_state(state[i], definition[i])
                for i in range(min(len(state), len(definition)))
            ]
        return state

    def _fetch_state(self, query: str, resource_type: str) -> dict:
        """Fetch the resource state as a dictionary."""
        row: BaseAdapter.Row = self.connection.execute(query)
        if not row:
            return {}

        logger.debug(
            f"state query output:\n{pformat(row, indent=2, width=80, compact=True, sort_dicts=False)}"
        )

        json_str = row[0] if isinstance(row, tuple) and len(row) > 0 else None
        if not json_str:
            raise RustyError(
                error="could not retrieve 'object_metadata' from the database query",
                file=str(Paths.state_file(resource_type)),
                help="Ensure the SQL query returns a column named 'object_metadata' "
                "containing a JSON string, or that the first column of the result "
                "contains the JSON string.",
            )

        try:
            return json.loads(json_str)
        except json.JSONDecodeError as err:
            raise RustyError(
                error="failed to parse JSON from database query result.",
                file=str(Paths.state_file(resource_type)),
                details=f"attempted to parse: {json_str}",
            ) from err

    @staticmethod
    def _check_state_keys(
        resource_type: str, definition: dict, template: dict, state: dict, name: str
    ) -> None:
        """Check state and definition keys match, including nested list items."""

        # 1. Check Top-Level State Keys
        # We expect the state to have everything defined in the provider template
        expected_top_keys = set(template.keys()) - {"wait_time", "depends_on", "name"}
        actual_state_keys = set(state.keys())
        missing_from_state = expected_top_keys - actual_state_keys

        if state and missing_from_state:
            raise RustyError(
                error=f"the state query for '{name}' is missing top-level arguments",
                file=str(Paths.state_file(resource_type)),
                help="Your SQL state query must return these keys in the JSON:\n"
                + "\n".join(f"  - {k}" for k in missing_from_state),
                note="The DDL template expects these keys to decide if an ALTER is needed.",
            )

        # 2. Check Nested Keys (e.g., columns)
        # If the template has a list of objects, the state must match that object schema
        for key, value in template.items():
            if (
                isinstance(value, list)
                and len(value) > 0
                and isinstance(value[0], dict)
            ):
                # This is a list of objects (like 'columns')
                expected_nested_keys = set(value[0].keys())

                # Check the state's version of this list
                state_items = state.get(key, [])
                if state_items and isinstance(state_items[0], dict):
                    actual_nested_keys = set(state_items[0].keys())
                    missing_nested = expected_nested_keys - actual_nested_keys

                    if missing_nested:
                        raise RustyError(
                            error=f"missing nested attributes in '{key}' for resource '{name}'",
                            file=str(Paths.state_file(resource_type)),
                            details=f"The list '{key}' in your state query output is missing fields.",
                            help=f"Update your SQL state query to include these fields inside the '{key}' objects:\n"
                            + "\n".join(f"  - {k}" for k in missing_nested),
                            note=f"Without these fields, the tool will always think '{key}' has drifted.",
                        )

        # 3. Check Definition vs State
        # Ensure the user didn't put something in their TOML that the SQL query isn't tracking
        user_keys = set(definition.keys()) - {"name", "depends_on", "wait_time"}
        untracked_keys = user_keys - actual_state_keys

        if state and untracked_keys:
            raise RustyError(
                error=f"definition for '{name}' contains untracked arguments",
                file=str(Paths.DEFINITIONS_DIR / f"{resource_type}.toml"),
                help="These arguments exist in your definition but are not returned by the state query:\n"
                + "\n".join(f"  - {k}" for k in untracked_keys),
                note="The drift detector cannot verify these fields because the state query ignores them.",
            )

    def _check_state_values(
        self, state: dict, definition: dict, template: dict
    ) -> DriftDDLContext:
        """Check the difference between state and definition values.

        Args:
            definition (str): Resource definition by the user
            template (str): Resource definition template
            state (str): State of the resource in the database
        """
        values_check_result = list(
            diff(first=state, second=definition, ignore={"name"})
        )
        logger.debug(
            f"state drift from the definition:\n{pformat(values_check_result, indent=2, width=80, compact=True, sort_dicts=False)}"
        )

        # no differences detected
        if not values_check_result:
            return DriftDDLContext(
                ddl_command=DDLCommand.NO_ACTION, name=definition.get("name", "")
            )

        drift_ddl_context = DriftDDLContext(
            ddl_command=DDLCommand.ALTER,
            name=definition.get("name", ""),
        )

        # If the state is empty, it's a CREATE operation
        if any(
            action == "add" and path == "" for action, path, _ in values_check_result
        ):
            return DriftDDLContext(
                add=definition,
                ddl_command=DDLCommand.CREATE,
                name=definition.get("name", ""),
            )

        drift_ddl_context = DriftDDLContext(
            ddl_command=DDLCommand.ALTER, name=definition.get("name", "")
        )

        for action, path, details in values_check_result:
            # Determine if this is a nested modification
            # If path is like ['any_list', 0, 'any_key'], it's a change to an existing item
            is_nested_item = (
                isinstance(path, list) and len(path) >= 2 and isinstance(path[1], int)
            )

            # DYNAMIC RE-MAPPING:
            # If we are adding a property to an existing list item, treat it as a 'change'
            current_action = (
                "change" if (action == "add" and is_nested_item) else action
            )

            if isinstance(path, list):
                root_key = path[0]
                if is_nested_item:
                    idx = path[1]
                    # Grab the whole item definition (e.g., the whole Column dict)
                    # so the template has all the context it needs to render the ALTER
                    value = definition[root_key][idx]
                    drift_ddl_context.add_frozen(current_action, root_key, value)
                else:
                    # Attribute change at the top level of the resource
                    # e.g., ('change', 'comment', (old, new))
                    drift_ddl_context[current_action][root_key] = details[1]

            elif isinstance(path, str):
                # Case: Top level attribute or whole item added/removed
                if action == "change":
                    drift_ddl_context[action][path] = details[1]
                elif action in {"add", "remove"}:
                    # details is a list of (index, value) for brand new items
                    added_items = {frozenset(value.items()) for _, value in details}
                    drift_ddl_context.union_frozen(action, path, added_items)

        return drift_ddl_context

    def resource_state(
        self,
        resource_type: str,
        definition: dict,
        template: dict,
        state_query: str,
        name: str,
    ) -> DriftDDLContext:
        """Compare the resource definition with the resource state."""
        rsc_def = self._normalize_definition(definition)
        logger.debug(
            f"definition args and values:\n{pformat(rsc_def, indent=2, width=80, compact=True, sort_dicts=False)}"
        )

        rsc_state = self._fetch_state(state_query, resource_type)

        self._check_state_keys(
            definition=rsc_def,
            resource_type=resource_type,
            template=template,
            state=rsc_state,
            name=name,
        )

        rsc_state = self._normalize_state(rsc_state, rsc_def)

        return self._check_state_values(
            definition=rsc_def,
            template=template,
            state=rsc_state,
        )
