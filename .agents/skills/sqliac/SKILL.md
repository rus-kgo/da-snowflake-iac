---
name: sqliac
description: >
  Generate `ddl_template.sql` and `state.sql` file pairs for database resources across any supported database engine.
license: MIT
---

## Purpose

Given a database engine (Snowflake, Oracle, PostgreSQL, etc.) and a resource type (table, view, role, etc.), produce:

1. **`ddl_template.sql`** — A Jinja-templated DDL script that creates, alters, or drops the resource.
2. **`state.sql`** — A query that retrieves the current state of the resource as a single JSON row whose keys exactly match the Jinja variables consumed by `ddl_template.sql`. Returns an empty result set if the resource does not exist.

## Project Structure

```
provider/
├── config.toml                 # Registry of all resources with example ddl_context
├── <resource_name>/
│   ├── ddl_template.sql        # Jinja DDL template
│   └── state.sql               # State introspection query
```

## Core Invariant

> **The keys returned by `state.sql` must be a 1:1 match with the Jinja variables expected by `ddl_template.sql`.**

If `ddl_template.sql` references `add.get('warehouse')`, then `state.sql` must return a JSON object containing the key `warehouse`. No extra keys, no missing keys.

## Step-by-Step Process

### 1. Identify DDL arguments

For the target resource, determine every configurable property that the DDL supports. Consult the database's official documentation. Categorize each property:

- **Required** — must always appear (e.g., `name`, `warehouse` for alerts)
- **Optional** — conditionally rendered with `{% if ... %}` guards

### 2. Determine DDL command strategy

| Strategy | When to use | Example |
|----------|-------------|---------|
| `CREATE OR ALTER` | The engine supports idempotent create-or-alter for this resource | Snowflake warehouses, views, tasks |
| `CREATE` / `ALTER` | No idempotent DDL exists; create uses `CREATE OR REPLACE`, alter uses `ALTER ... SET` | Snowflake dynamic tables |
| `GRANT` / `REVOKE` | Privilege management (inherently idempotent) | All engines |

**Prefer the simplest idempotent command available.** This minimizes the template's branching logic.

### 3. Write `ddl_template.sql`

#### Single Create and Alter Statement Example
```sql
{% set add = add | default({}) %}
{% set change = change | default({}) %}
{% set remove = remove | default({}) %}

{% set optional_prop = 'optional_prop' | pick(add, change) %}

{% if ddl_command.upper() == 'CREATE OR ALTER' %}
CREATE OR ALTER <RESOURCE> {{ name }}
  <REQUIRED_PROPS>
  {% if optional_prop %}
  OPTIONAL_PROP = {{ optional_prop }}
  {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP <RESOURCE> {{ name }};
{% endif -%}
```

#### Separate Create or Alter Statements Example
```sql
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE <RESOURCE> {{ name }}
    <REQUIRED_PROPS>
    {% if add.get('optional_prop') is not none %}
    OPTIONAL_PROP = {{ add.optional_prop | sql_escape }}
    {% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
ALTER <RESOURCE> {{ name }} SET 
    {% if change.get('optional_prop') is not none %}
    OPTIONAL_PROP = {{ change.optional_prop | sql_escape }}
    {% endif %};

{% elif ddl_command.upper() == 'DROP' %}
DROP <RESOURCE> {{ name }};
{% endif -%}
```

#### Nested Props Example
```sql
#### Nested props
-- DDL template for <Engine> <Resource> resource
{% set add = add | default({}) %}
{% set change = change | default({}) %}
{% set remove = remove | default({}) %}

{% set add_items = add.get('nested_prop', []) %}
{% set remove_items = remove.get('nested_prop', []) %}
{% set optional_prop = 'optional_prop' | pick(add, change) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE <RESOURCE> {{ name }}
  {% if add_items %} (
  {% for item in add_items %}
      {{ item.prop }}{% if not loop.last %},{% endif %}
  {% endfor %}
  ) {% endif %}
  {% if optional_prop %}
  OPTIONAL_PROP = {{ optional_prop | sql_escape }}
  {% endif %};

{% elif ddl_command.upper() == 'ALTER' %}

{# Handle Adding New Items #}
{% for item in add_items %}
ALTER <RESOURCE> {{ name }} ADD PROP {{ item.prop }};
{% endfor %}

{# Handle Removing Items #}
{% for item in remove_items %}
ALTER <RESOURCE> {{ name }} DROP PROP {{ item.prop }};
{% endfor %}

{% elif ddl_command.upper() == 'DROP' %}
DROP <RESOURCE> {{ name }};
{% endif -%}
```

**Rules:**
- Always declare `{% set add = add | default({}) %}`, `{% set change = change | default({}) %}` and `{% set remove = remove | default({}) %}` at the top.
- Use `add.get('optional_prop')` for create flows
- Use `change.get('optional_prop')` or `remove.get('optional_prop')` for alter flows
- Use the pick filter for any property that exists in both add and change. This creates the cleanest, most readable templates.  
  ```sql
  {% set data_retention = 'data_retention_time_in_days' | pick(add, change) %}
  {% if data_retention is not none %}
    DATA_RETENTION_TIME_IN_DAYS = {{ data_retention }}
  {% endif %}
  ```
- Quote string values with single quotes in the DDL output.
- For booleans, emit raw `true`/`false` (no quotes) unless the engine requires otherwise.

### 4. Write `state.sql`

The state query must:
- Return **exactly one row** with a single JSON column (`object_metadata`) when the resource exists.
- Return **zero rows** (empty result set) when it does not exist — never an error.
- Contain keys matching every argument used in `ddl_template.sql`.
- Database engines often return type aliases (e.g., Snowflake returns TEXT for VARCHAR). To avoid infinite drift loops, you must normalize these in the SQL query.  
  ```sql
  SELECT object_construct_keep_null(
      'type', CASE 
          WHEN DATA_TYPE = 'TEXT' THEN 'VARCHAR' 
          WHEN DATA_TYPE = 'NUMBER' THEN 'NUMBER' -- Handle precision/scale if needed
          ELSE DATA_TYPE 
      END,
      'nullable', (IS_NULLABLE = 'YES') -- Returns a boolean true/false
  ) ...
  ```

#### Choosing the right introspection method

| Scenario | Technique |
|----------|-----------|
| All properties available from a single `SHOW` command | Use SHOW + filter |
| Some properties only available via `DESCRIBE` or system views | Use DESCRIBE and pivot rows into columns via `CASE WHEN` |
| Properties split across multiple commands | Chain commands in a procedural block (cursor for first, then second query) |

#### Snowflake patterns

**Simple (SHOW exposes everything):**
```sql
{% set name_parts = name.split('.') %}
EXECUTE IMMEDIATE $$
BEGIN
    SHOW <RESOURCES> LIKE '{{ name_parts[1] }}' IN {{ name_parts[0] }};

    LET rs RESULTSET := (
        SELECT object_construct_keep_null(
            'key1', "column1",
            'key2', "column2"
        ) AS object_metadata
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "name" = '{{ name_parts[0] }}'
        LIMIT 1
    );
    RETURN TABLE(rs);
EXCEPTION
    WHEN OTHER THEN
        LET rs_empty RESULTSET := (SELECT NULL AS object_metadata WHERE 1=0);
        RETURN TABLE(rs_empty);
END;
$$;
```

**DESCRIBE pivot (for integrations and objects with key-value property output):**
```sql
{% set name_parts = name.split('.') %}
EXECUTE IMMEDIATE $$
BEGIN
    
    DESCRIBE <RESOURCE> {{ name_parts[2] }};

    LET rs RESULTSET := (
        WITH props AS (
            SELECT
                MAX(CASE WHEN "property" = 'PROP_A' THEN "property_value" END) AS prop_a,
                MAX(CASE WHEN "property" = 'PROP_B' THEN "property_value" END) AS prop_b
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        )
        SELECT object_construct_keep_null(
            'prop_a', props.prop_a,
            'prop_b', NULLIF(props.prop_b, '')
        ) AS object_metadata
        FROM props
    );
    RETURN TABLE(rs);
EXCEPTION
    WHEN OTHER THEN
        LET rs_empty RESULTSET := (SELECT NULL AS object_metadata WHERE 1=0);
        RETURN TABLE(rs_empty);
END;
$$;
```

**Multi-command (combining two SHOW commands):**
```sql
EXECUTE IMMEDIATE $$
BEGIN
    SHOW <RESOURCE_A> LIKE '...' IN ...;
    -- Capture into variables via cursor
    LET v_col1 VARCHAR;
    LET c1 CURSOR FOR SELECT "col1" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())) WHERE ...;
    OPEN c1; FETCH c1 INTO v_col1; CLOSE c1;

    SHOW <RESOURCE_B> LIKE '...' IN ...;
    LET rs RESULTSET := (
        SELECT object_construct_keep_null(
            'key_from_a', :v_col1,
            'key_from_b', "col_from_b"
        ) AS object_metadata
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "name" = '...'
        LIMIT 1
    );
    RETURN TABLE(rs);
EXCEPTION
    WHEN OTHER THEN
        LET rs_empty RESULTSET := (SELECT NULL AS object_metadata WHERE 1=0);
        RETURN TABLE(rs_empty);
END;
$$;
```

#### Oracle patterns

```sql
-- State query using Oracle data dictionary
SELECT JSON_OBJECT(
    'tablespace_name' VALUE t.tablespace_name,
    'logging'         VALUE t.logging,
    'block_size'      VALUE t.block_size
    RETURNING CLOB
) AS object_metadata
FROM dba_tablespaces t
WHERE t.tablespace_name = UPPER('{{ name }}');
-- Returns zero rows if tablespace does not exist
```

#### PostgreSQL patterns

```sql
-- State query using information_schema / pg_catalog
SELECT json_build_object(
    'owner', r.rolname,
    'encoding', pg_encoding_to_char(d.encoding),
    'lc_collate', d.datcollate,
    'connection_limit', d.datconnlimit
) AS object_metadata
FROM pg_database d
JOIN pg_roles r ON r.oid = d.datdba
WHERE d.datname = '{{ name }}';
-- Returns zero rows if database does not exist
```

### 5. Register in `config.toml`

Add a section for the new resource following this pattern:

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

## Validation Checklist

Before considering a resource complete, verify:

- [ ] Every key in `ddl_context` (except `name`, `wait_time`, `depends_on`) appears in `ddl_template.sql`
- [ ] Every key referenced via `add.get('X')` or `change.get('X')` in `ddl_template.sql` is returned by `state.sql`
- [ ] `state.sql` does **not** return keys that `ddl_template.sql` never uses
- [ ] `state.sql` returns empty (zero rows) on resource-not-found — not an error
- [ ] String values use `NULLIF(..., '')` to normalize empty strings to NULL
- [ ] Boolean values are derived from comparison expressions (e.g., `"enabled" = 'true'`)
- [ ] The DDL template handles both `add` and `change` contexts via `or` chaining

## Common Mistakes to Avoid

1. **Using only `SHOW` when it doesn't expose all properties.** Many database metadata commands return a subset. Always cross-reference against the DDL's full argument list and use `DESCRIBE` or system views when SHOW falls short.

2. **Returning extra keys from `state.sql`.** If the DDL template doesn't use `state` or `type`, don't include them. Extra keys pollute the diff logic.

3. **Forgetting optional properties.** If `ddl_template.sql` conditionally renders `ENCRYPTION = (...)` then `state.sql` must return `encryption_type` even if the value is NULL.

4. **Hardcoding NULLs for provider-specific properties.** For resources with provider variants (e.g., AWS vs Azure vs GCP integrations), return all variant keys — the unused ones will be NULL via `NULLIF`.

## Engine-Specific Notes

### Snowflake
- Use `EXECUTE IMMEDIATE $$ BEGIN ... END; $$` wrappers for multi-statement logic.
- `TABLE(RESULT_SCAN(LAST_QUERY_ID()))` captures SHOW/DESCRIBE output.
- Exception handler returns empty result set: `SELECT NULL AS object_metadata WHERE 1=0`.
- Jinja variables for schema-scoped objects: `{% set name_parts = name.split('.') %}`.

### Oracle
- Use data dictionary views (`DBA_*`, `ALL_*`, `USER_*`).
- `JSON_OBJECT(... RETURNING CLOB)` for JSON output.
- Wrap in PL/SQL anonymous block if multi-step introspection is needed.
- Handle not-found by relying on the WHERE clause returning zero rows.

### PostgreSQL
- Use `pg_catalog` and `information_schema` views.
- `json_build_object(...)` for JSON construction.
- Use `LEFT JOIN` patterns when some properties may not exist.
- Handle not-found naturally (WHERE clause returns zero rows).

### MySQL
- Use `INFORMATION_SCHEMA` tables.
- `JSON_OBJECT(...)` for JSON output.
- `SHOW CREATE <RESOURCE>` can be parsed for properties not in INFORMATION_SCHEMA.
