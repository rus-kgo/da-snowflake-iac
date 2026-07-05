---
name: sqliac
description: >
  Generate `ddl_template.sql` and `state.sql` file pairs for database resources
  (table, view, role, warehouse, grant, etc.) across any supported SQL engine
  — Snowflake, Oracle, PostgreSQL, and Databricks. Trigger whenever the user
  asks to "create <engine> templates for <resource>", add a new resource type
  to a provider, or write Jinja+SQL IaC template pairs. Also use when asked to
  validate that an existing ddl_template.sql / state.sql pair matches the
  project's key-parity invariant.
license: MIT
---

## SQLIaC — Jinja+SQL Templates Generation Skill

Given an **engine** (Snowflake, Oracle, PostgreSQL, Databricks) and a
**resource type** (table, view, role, warehouse, grant, etc.), produce:

1. **`ddl_template.sql`** — a Jinja-templated DDL script that creates, alters,
   or drops the resource.
2. **`state.sql`** — a query that returns the resource's current state as a
   single JSON row whose keys exactly match the Jinja variables consumed by
   `ddl_template.sql`. Returns an empty result set if the resource doesn't exist.

## Project Structure

```
provider/
├── config.toml                 # Registry of all resources with example ddl_context
├── <resource_name>/
│   ├── ddl_template.sql        # Jinja DDL template
│   └── state.sql               # State introspection query
```

## Core Invariant (applies to every engine)

> **The keys returned by `state.sql` must be a 1:1 match with the Jinja
> variables expected by `ddl_template.sql`.**

If `ddl_template.sql` references `add.get('warehouse')`, then `state.sql`
must return a JSON object containing the key `warehouse`. No extra keys, no
missing keys. Run `scripts/check_key_parity.py` against the pair before
calling a resource done — see [Validate the pair](#validate-the-pair).

## Step 0 — Pick the engine reference

**Read the matching reference file before writing any template.** Each one
holds the engine's DDL command strategy, introspection technique, and worked
examples — the specifics genuinely differ per engine and don't belong in this
file.

| User says... | Read |
|---|---|
| "snowflake", "sf" | `references/snowflake.md` |
| "oracle" | `references/oracle.md` |
| "postgres", "postgresql", "pg" | `references/postgresql.md` |
| "databricks", "unity catalog", "spark sql" | `references/databricks.md` |

If the engine isn't named explicitly but the project already has a
`.sqliac/provider/config.toml` with a `[<engine>.*]` top-level table, infer
the engine from that file instead of asking.

If none of the four match, say so — don't guess at syntax for an
unsupported engine.

## Step 1 — Identify DDL arguments

For the target resource, determine every configurable property the DDL
supports. Consult the engine's official documentation (or the reference
file, which already curates the common resources). Categorize each property:

- **Required** — must always appear (e.g., `name`, `warehouse` for a
  Snowflake alert)
- **Optional** — conditionally rendered with `{% if ... %}` guards

## Step 2 — Determine DDL command strategy

The reference file's "DDL command strategy" table tells you which of these
applies per resource on that engine:

| Strategy | When to use |
|----------|-------------|
| `CREATE OR ALTER` (or engine equivalent, e.g. `CREATE OR REPLACE`) | The engine supports idempotent create-or-alter for this resource |
| `CREATE` / `ALTER` | No idempotent DDL exists |
| `GRANT` / `REVOKE` | Privilege management (inherently idempotent, same on every engine) |

**Prefer the simplest idempotent command available** — it minimizes the
template's branching logic.

## Step 3 — Write `ddl_template.sql`

Structural rules are engine-agnostic; syntax comes from the reference file.

**Always start with:**
```sql
{% set add = add | default({}) %}
{% set change = change | default({}) %}
{% set remove = remove | default({}) %}
```

**Rules:**
- Use `add.get('optional_prop')` for create flows, `change.get('optional_prop')`
  / `remove.get('optional_prop')` for alter flows.
- Use the `pick` filter for any property that exists in both `add` and
  `change` — cleanest, most readable templates:
  ```sql
  {% set data_retention = 'data_retention_time_in_days' | pick(add, change) %}
  {% if data_retention is not none %}
    DATA_RETENTION_TIME_IN_DAYS = {{ data_retention }}
  {% endif %}
  ```
- Quote string values with single quotes in the DDL output.
- Emit raw `true`/`false` for booleans (no quotes) unless the engine
  requires otherwise (see reference file).
- The three shapes below cover almost every resource — pick the one that
  fits and check the reference file for the engine's exact keywords:
  - **Single create/alter statement** (schema, warehouse, role)
  - **Separate CREATE / ALTER statements** (user, storage integration)
  - **Nested props** — list-valued fields like table columns, needing
    per-item ADD/DROP handling on alter

## Step 4 — Write `state.sql`

The state query must, regardless of engine:
- Return **exactly one row** with a single JSON column (`object_metadata`)
  when the resource exists.
- Return **zero rows** when it does not exist — never an error.
- Contain keys matching every argument used in `ddl_template.sql`.
- Normalize engine type aliases (e.g., Snowflake's `TEXT` → `VARCHAR`) so
  drift detection doesn't loop forever on cosmetic differences.
- Normalize empty strings to `NULL` via the engine's `NULLIF`-equivalent.

Each reference file has a "Choosing the right introspection method" table
(single `SHOW`/system-view query vs. `DESCRIBE`-pivot vs. multi-step
procedural block) plus worked examples for that engine's common resources.

## Step 5 — Register in `config.toml`

```toml
[<engine>.<resource_name>.ddl_command]
create = "<CREATE_COMMAND>"
alter  = "<ALTER_COMMAND>"
drop   = "<DROP_COMMAND>"

[<engine>.<resource_name>.ddl_context]
# Example values for each argument — serves as documentation and test fixture
name = "FULLY.QUALIFIED.NAME"
<key1> = <example_value>
<key2> = <example_value>
wait_time = 0

[<engine>.<resource_name>.ddl_context.depends_on]
```

`assets/config_entry_template.toml` has this as a copy-paste starting point.

## Validate the pair

Before considering a resource complete, run:

```bash
python3 .agents/skills/sqliac/scripts/check_key_parity.py \
  <path-to>/ddl_template.sql <path-to>/state.sql
```

It extracts every `add.get('x')` / `change.get('x')` / `remove.get('x')` key
referenced in the DDL template and every top-level JSON key produced by the
state query, then reports mismatches in both directions. It's a heuristic
(regex-based, not a real Jinja/SQL parser) — treat a clean run as a strong
signal, not a formal proof, and still eyeball the two files.

Manual checklist (also what the script checks):

- [ ] Every key in `ddl_context` (except `name`, `wait_time`, `depends_on`)
      appears in `ddl_template.sql`
- [ ] Every key referenced via `add.get('X')` / `change.get('X')` in
      `ddl_template.sql` is returned by `state.sql`
- [ ] `state.sql` does **not** return keys `ddl_template.sql` never uses
- [ ] `state.sql` returns empty (zero rows) on resource-not-found
- [ ] The DDL template handles both `add` and `change` contexts
      (via `or`-chaining or the `pick` filter)

## Common Mistakes to Avoid

1. **Using only the engine's "list" command when it doesn't expose all
   properties.** Cross-reference against the DDL's full argument list and
   fall back to a describe/pivot or system-view query — see the reference
   file's introspection table.
2. **Returning extra keys from `state.sql`.** If the DDL template doesn't
   use a property, don't return it. Extra keys pollute the diff logic.
3. **Forgetting optional properties.** If `ddl_template.sql` conditionally
   renders a clause, `state.sql` must still return that key (as `NULL` when
   absent).
4. **Hardcoding NULLs for provider-variant properties.** For resources with
   variants (e.g., AWS vs. Azure vs. GCP integrations), return all variant
   keys — unused ones resolve to `NULL`.
5. **Copying syntax across engines.** `EXECUTE IMMEDIATE $$ ... $$` is
   Snowflake-only; Oracle wants a PL/SQL block; PostgreSQL wants `DO $$ ...
   $$`; Databricks mostly doesn't need one at all. Always confirm against
   the reference file rather than assuming the pattern transfers.
