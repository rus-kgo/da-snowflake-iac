from typing import Any

from dictdiffer import diff
from icecream import ic

from sqliac.constants import DDLCommand
from sqliac.drift import DriftDDLContext

definition = {
    "name": "MODELING.MODELING_SCHEMA.FIRST_TABLE",
    "columns": [
        {
            "name": "COLUMN1",
            "type": "VARCHAR",
            "nullable": True,
            "comment": "COLUMN 1 DESCRIPTION",
        },
        {
            "name": "COLUMN2",
            "type": "NUMBER",
            "nullable": False,
            "default": 0,
            "comment": "COLUMN 2 DESCRIPTION",
        },
        {
            "name": "COLUMN3",
            "type": "TEXT",
            "nullable": True,
            "default": 0,
            "comment": "COLUMN 3 DESCRIPTION",
        },
    ],
}

state = {
    "columns": [
        {"comment": None, "name": "COLUMN1", "nullable": True, "type": "TEXT"},
        {
            "comment": None,
            "name": "COLUMN2",
            "nullable": False,
            "default": 0,
            "type": "NUMBER",
        },
        {
            "comment": None,
            "name": "COLUMN3",
            "nullable": True,
            "default": 0,
            "type": "TEXT",
        },
    ],
    "comment": None,
}

ignore_paths = set(definition.keys()) ^ set(state.keys())
ignore_paths.add("name")

differnce = diff(first=state, second=definition)
ic(differnce)

values_check_result = [
    ("change", ["columns", 0, "comment"], (None, "COLUMN 1 DESCRIPTION")),
    ("change", ["columns", 0, "type"], ("TEXT", "VARCHAR")),
    ("change", ["columns", 1, "comment"], (None, "COLUMN 2 DESCRIPTION")),
    ("add", ["columns", 1], [("default", 0)]),
    ("change", ["columns", 2, "comment"], (None, "COLUMN 3 DESCRIPTION")),
    ("add", ["columns", 2], [("default", 0)]),
]

drift_ddl_context = DriftDDLContext(
    ddl_command=DDLCommand.ALTER,
    name=definition.get("name", ""),
)

# If the state is empty, it's a CREATE operation
if any(action == "add" and path == "" for action, path, _ in values_check_result):
    ic(
        DriftDDLContext(
            add=definition,
            ddl_command=DDLCommand.CREATE,
            name=definition.get("name", ""),
        )
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
    current_action = "change" if (action == "add" and is_nested_item) else action

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
            added_items = {value for _, value in details}
            drift_ddl_context.union_frozen(action, path, added_items)

ic(drift_ddl_context)


def _filter_state_by_definition(state: Any, definition: Any) -> Any:
    """Recursively filter state to only include keys present in definition."""
    if isinstance(definition, dict) and isinstance(state, dict):
        return {
            k: _filter_state_by_definition(state[k], v)
            for k, v in definition.items()
            if k in state
        }
    elif isinstance(definition, list) and isinstance(state, list):
        # For lists, we align by index
        return [
            _filter_state_by_definition(state[i], definition[i])
            for i in range(min(len(state), len(definition)))
        ]
    return state


if __name__ == "__main__":
    ic(_filter_state_by_definition(state, definition))
    #
    # dict1 = {"item": "apple", "price": 1.20}
    # dict2 = {"item": "banana", "price": 0.50}
    # dict3 = {"item": "apple", "price": 1.20}  # Duplicate of dict1

    # # Convert key-value items into frozenset to make them hashable
    # set_of_dicts = {frozenset(d.items()) for d in [dict1, dict2, dict3]}

    # # Output will automatically deduplicate and contain only 2 unique items
    # print(set_of_dicts)
