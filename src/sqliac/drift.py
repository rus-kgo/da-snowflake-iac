"""State comparison module.

This module performs the checks of sql database resources drift.
Drift is the term for when the real-world state of your infrastructure differs from the state defined in your configuration.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
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

    def _fetch_state(self, query: str, resource_type: str) -> dict:
        """Fetch the resource state as a dictionary."""
        row: BaseAdapter.Row = self.connection.execute(query)
        if not row:
            return {}

        logger.debug(f"state query output:{row}")

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
        """Check state and definition keys match.

        Args:
            resource_type(str): Resource type (e.g. database)
            definition(dict): Resource definition by the user
            template(dict): Resource definition template
            state(dict): State of the resource in the database
        """
        invalid_state_keys = set(state.keys()) ^ set(
            (template.keys() - {"wait_time", "depends_on", "name"})
        )
        logger.debug(f"state output vs ddl_cotext in the config:{invalid_state_keys}")

        if invalid_state_keys:
            invalid_state_keys_str = "\n".join(f"  - {k}" for k in invalid_state_keys)
            raise RustyError(
                error=f"invalid or missing '{name}' state output arguments",
                file=str(Paths.state_file(resource_type)),
                help=f"""review the arguments in your state query and config DDL context
{invalid_state_keys_str}""",
                note="the state query output must match the DDL context in the config",
            ) from None

        invalid_defin_keys = (
            (set(definition.keys()) - {"name"}) - set(state.keys()) if state else None
        )
        logger.debug(f"state output vs definition args:{invalid_defin_keys}")

        if invalid_defin_keys:
            invalid_defin_keys_str = "\n".join(f"  - {k}" for k in invalid_defin_keys)
            raise RustyError(
                error=f"invalid or missing '{name}' definition arguments",
                file=str(Paths.DEFINITIONS_DIR / f"{resource_type}.toml"),
                help=f"""add the missing arguments to your definition file or remove invalid ones
{invalid_defin_keys_str}""",
                note="the state query output must match the definition arguments",
            ) from None

    def _check_state_values(
        self, state: dict, definition: dict, template: dict
    ) -> DriftDDLContext:
        """Check the difference between state and definition values.

        Args:
            definition (str): Resource definition by the user
            template (str): Resource definition template
            state (str): State of the resource in the database
        """
        ignore_paths = set(definition.keys()) ^ set(template.keys())
        ignore_paths.add("name")

        values_check_result = list(
            diff(first=state, second=definition, ignore=ignore_paths)
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
        template: dict,
        state_query: str,
        name: str,
    ) -> DriftDDLContext:
        """Compare the resource definition with the resource state."""
        rsc_def = self._normalize_definition(definition)

        rsc_state = self._fetch_state(state_query, resource_type)

        rsc_state = self._normalize_definition(rsc_state)

        self._check_state_keys(
            definition=rsc_def,
            resource_type=resource_type,
            template=template,
            state=rsc_state,
            name=name,
        )

        return self._check_state_values(
            definition=rsc_def,
            template=template,
            state=rsc_state,
        )
