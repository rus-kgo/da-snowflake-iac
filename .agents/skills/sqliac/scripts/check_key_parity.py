#!/usr/bin/env python3
"""Check the sqliac Core Invariant: state.sql keys == ddl_template.sql keys.

Usage:
    python3 check_key_parity.py <ddl_template.sql> <state.sql>

This is a regex-based heuristic, not a real Jinja/SQL parser. It's meant to
catch the common mistake (forgetting a key on one side) before you hand-eyeball
the files, not to formally prove correctness. A clean run is a strong signal;
it is not a guarantee.

Exit code 0 = keys match. Exit code 1 = mismatch found (also printed).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Keys referenced via add.get('x'), change.get('x'), remove.get('x')
GET_PATTERN = re.compile(r"(?:add|change|remove)\.get\(\s*['\"](\w+)['\"]")

# Keys referenced via dot access, e.g. add.optional_prop (excludes .get itself)
DOT_PATTERN = re.compile(r"(?:add|change|remove)\.(?!get\b)(\w+)")

# Keys referenced via the `pick` filter: 'x' | pick(add, change)
PICK_PATTERN = re.compile(r"['\"](\w+)['\"]\s*\|\s*pick\(")

# Keys produced by JSON constructors across engines. Single-quote only —
# double-quoted tokens are SQL identifiers (e.g. Snowflake's "column_name"),
# not JSON keys, in every engine this skill targets:
#   Snowflake:   object_construct_keep_null('key', val, ...)
#   Postgres:    json_build_object('key', val, ...)
#   Databricks:  to_json(named_struct('key', val, ...))  -- same 'key', shape
#   Oracle:      JSON_OBJECT('key' VALUE val, ...)
JSON_KV_PATTERN = re.compile(r"'(\w+)'\s*(?:,|VALUE\b)", re.IGNORECASE)

IGNORED_KEYS = {"name", "depends_on", "wait_time"}

# Quoted string literals that occasionally land in the "key" position by
# regex coincidence (e.g. `= 'true'` followed by a comma to the next pair).
# Not exhaustive — the note in main() covers what this doesn't catch.
LITERAL_NOISE = {"true", "false", "yes", "no", "null"}


def extract_ddl_keys(text: str) -> set[str]:
    keys = set()
    keys.update(GET_PATTERN.findall(text))
    keys.update(DOT_PATTERN.findall(text))
    keys.update(PICK_PATTERN.findall(text))
    return keys - IGNORED_KEYS


def extract_state_keys(text: str) -> set[str]:
    found = {k.lower() for k in JSON_KV_PATTERN.findall(text)}
    return found - IGNORED_KEYS - LITERAL_NOISE


def main() -> int:
    if len(sys.argv) != 3:
        print(__doc__)
        return 2

    ddl_path, state_path = Path(sys.argv[1]), Path(sys.argv[2])
    ddl_keys = extract_ddl_keys(ddl_path.read_text())
    state_keys = extract_state_keys(state_path.read_text())

    missing_from_state = sorted(ddl_keys - state_keys)
    extra_in_state = sorted(state_keys - ddl_keys)

    if not missing_from_state and not extra_in_state:
        print(f"OK: {len(ddl_keys)} keys match between {ddl_path.name} and {state_path.name}")
        return 0

    if missing_from_state:
        print(f"Missing from {state_path.name} (referenced in {ddl_path.name} but not returned):")
        for key in missing_from_state:
            print(f"  - {key}")

    if extra_in_state:
        print(f"Extra in {state_path.name} (returned but never used in {ddl_path.name}):")
        for key in extra_in_state:
            print(f"  - {key}")

    print("\nNote: this is a regex heuristic — double-check before trusting a clean result blindly,")
    print("and manually verify any flagged key, since string literals unrelated to JSON keys can occasionally match.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
