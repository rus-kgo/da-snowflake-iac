"""State comparison module.

This module performs the checks of sql database resources drift.
Drift is the term for when the real-world state of your infrastructure differs from the state defined in your configuration.
"""

from __future__ import annotations

import json
from typing import Any, TYPE_CHECKING
from dictdiffer import diff
from dataclasses import dataclass, field, asdict

if TYPE_CHECKING:
    from sqliac.adapters.base import BaseAdapter

from sqliac.value_sanitizer import ValueSanitizer
from sqliac.errors import RustyError
from sqliac import DDLCommand


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
    add: dict[str, Any] = field(default_factory=dict)
    change: dict[str, Any] = field(default_factory=dict)
    remove: dict[str, Any] = field(default_factory=dict)

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

    def append(self, action: str, key: str, value: Any) -> None:  # noqa: D102
        section = getattr(self, action)
        section.setdefault(key, []).append(value)

    def extend_values(  # noqa: D102
        self,
        action: str,
        key: str,
        values: list[Any],
    ) -> None:
        section = self[action]
        section.setdefault(key, []).extend(values)

    def to_dict(self):
        """Return as dictionary."""
        return asdict(self)


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

        return {
            key: self.sanitizer.deep_clean(value) for key, value in ddl_context.items()
        }

    def _fetch_state(self, query: str) -> dict:
        """Fetch the resource state as a dictionary."""
        data = self.connection.execute(query)
        if not data:
            return {}

        row = data[0]

        json_str = row.get("object_metadata") if isinstance(row, dict) else row[0]

        if not json_str:
            state_query = """
            SELECT object_construct(
                'name', database_name,
                'owner', database_owner,
                'transient', type = 'TRANSIENT',
                'data_retention_time_in_days', retention_time,
                'comment', comment,
                'created_on', created
            ) as object_metadata
            FROM information_schema.databases
            WHERE database_name = '{{ name }}'
            LIMIT 1
            """
            raise RustyError(
                error="failed to parse JSON from state query",
                file="./resources.toml",
                help="make sure that state query SQL template returns "
                "`object_metadata` column with a single row JSON object"
                f"\n   Snowflake database example:\n{state_query}",
            )

        try:
            return json.loads(json_str)
        except json.JSONDecodeError as err:
            raise RustyError(
                error="failed to parse JSON from state query",
                file="./resources.toml",
            ) from err

    def _check_state_keys(
        self, resource_type: str, definition: dict, state: dict, name: str
    ) -> None:
        """Check state and definition keys match.

        Args:
            resource_type (str): Dabatabase resource type
            definition (dict): Resource definiton by the user
            state (dict): State of the resource in the database
            name (str): Resource identifier in the database
        """
        keys_check_result = list(diff(first=definition, second=state))

        invalid_keys = set()
        missing_keys = set()

        # missing  and invalid keys will have no path
        for action, path, details in keys_check_result:
            if state == {}:
                continue

            if path != "":
                continue

            # ('add', '', [('wrong', '')]),
            if action == "add":
                for tpl in details:
                    key = tpl[0] if len(tpl) > 1 else None
                    invalid_keys.add(key)

            # ('remove', '', [('schema', 'NEW')])]
            elif action == "remove":
                for tpl in details:
                    key = tpl[0] if len(tpl) > 1 else None
                    missing_keys.add(key)

        missing_invalid_keys = invalid_keys | missing_keys

        if missing_invalid_keys:
            missing_invalid_keys_str = "\n".join(
                f"  - {k}" for k in missing_invalid_keys
            )
            block = f"""\
        --> {resource_type}.yml
        |
        1 | [[{resource_type}]]
        2 | name = "{name}"
        |       ^^^ incorrect definition
        |
        """
            raise RustyError(
                error="invalid or missing resource definition arguments",
                file=f"{resource_type}.toml",
                details=block,
                help="add the missing arguments to your definition file or "
                f"remove invalid ones\n    {missing_invalid_keys_str}",
            )

    def _check_state_values(self, state: dict, definition: dict) -> DriftDDLContext:
        """Check the difference between state and definition values.

        Args:
            definition (str): Resource definiton by the user
            state (str): State of the resource in the database
        """
        values_check_result = list(
            diff(first=state, second=definition, ignore={"name"})
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
        for action, path, details in values_check_result:
            # the state output is and empty dictionary - the object does not exsists
            if action == "add" and path == "":
                return DriftDDLContext(
                    add=definition,
                    ddl_command=DDLCommand.CREATE,
                    name=definition.get("name", ""),
                )

            # ('change', ['columns', 1, 'name'], ('state change', 'COLUMN2')),
            if isinstance(path, list) and len(path) >= 2:  # noqa: PLR2004
                key, idx = path[0], path[1]

                # accept definition-side value only
                value = definition[key][idx]

                drift_ddl_context.append(action, key, value)

            elif isinstance(path, str):
                # ('change', 'database', ('MAIN', 'NEW'))
                if action == "change":
                    drift_ddl_context[action][path] = details[1]

                # ('add', 'columns', [(2, {...})])
                elif action in {"add", "remove"}:
                    added_items = [value for _, value in details]

                    drift_ddl_context.extend_values(action, path, added_items)

        return drift_ddl_context

    def resource_state(
        self,
        resource_type: str,
        definition: dict,
        state_query: str,
        name: str,
    ) -> DriftDDLContext:
        """Compare the resource definition with the resource state."""
        rsc_def = self._normalize_definition(definition)

        rsc_state = self._fetch_state(state_query)

        rsc_state = self._normalize_definition(rsc_state)

        self._check_state_keys(
            definition=rsc_def,
            resource_type=resource_type,
            state=rsc_state,
            name=name,
        )

        return self._check_state_values(
            definition=rsc_def,
            state=rsc_state,
        )
