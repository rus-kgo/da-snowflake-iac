# PostgreSQL Reference

PostgreSQL has no `CREATE OR ALTER` either, but `CREATE OR REPLACE` covers
views, functions, and procedures — treat those as idempotent-create and
everything else (tables, roles, databases) as separate CREATE/ALTER.

## DDL command strategy by resource

| Strategy | Resources |
|----------|-----------|
| `CREATE OR REPLACE` | view, function, procedure, trigger |
| `CREATE` / `ALTER` (separate) | table, database, role, schema, extension, sequence, index |
| `GRANT` / `REVOKE` | grant |

## Template shape (CREATE OR REPLACE resources)

```sql
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR REPLACE' %}
CREATE OR REPLACE <RESOURCE> {{ name }}
  <REQUIRED_PROPS>
  AS
    {{ add.get('query') or change.get('query') }};

{% elif ddl_command.upper() == 'DROP' %}
DROP <RESOURCE> IF EXISTS {{ name }};
{% endif -%}
```

## Template shape (separate CREATE/ALTER resources)

```sql
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE <RESOURCE> {{ name }}
    <REQUIRED_PROPS>
    {% if add.get('optional_prop') is not none %}
    <OPTIONAL_PROP_CLAUSE> {{ add.optional_prop | sql_escape }}
    {% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
{% if change.get('optional_prop') is not none %}
ALTER <RESOURCE> {{ name }} SET <OPTIONAL_PROP_CLAUSE> = {{ change.optional_prop | sql_escape }};
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP <RESOURCE> IF EXISTS {{ name }};
{% endif -%}
```

`IF EXISTS` on drop is idiomatic Postgres and safe to use unconditionally —
it makes destroy operations idempotent without extra template logic.

## Nested props example (table columns)

```sql
{% set add_cols = add.get('columns', []) %}
{% set change_cols = change.get('columns', []) %}
{% set remove_cols = remove.get('columns', []) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE TABLE {{ name }} (
{% for column in add_cols %}
    {{ column.name }} {{ column.type }}{% if not column.get('nullable', true) %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}
{% endfor %}
);

{% elif ddl_command.upper() == 'ALTER' %}
{% for column in add_cols %}
ALTER TABLE {{ name }} ADD COLUMN {{ column.name }} {{ column.type }};
{% endfor %}
{% for column in change_cols %}
{% if column.get('type') %}
ALTER TABLE {{ name }} ALTER COLUMN {{ column.name }} TYPE {{ column.type }};
{% endif %}
{% if column.get('nullable') is not none %}
ALTER TABLE {{ name }} ALTER COLUMN {{ column.name }} {% if column.nullable %}DROP{% else %}SET{% endif %} NOT NULL;
{% endif %}
{% endfor %}
{% for column in remove_cols %}
ALTER TABLE {{ name }} DROP COLUMN {{ column.name }};
{% endfor %}

{% elif ddl_command.upper() == 'DROP' %}
DROP TABLE IF EXISTS {{ name }};
{% endif -%}
```

## Choosing the right introspection method

| Scenario | Technique |
|----------|-----------|
| `information_schema` has everything | Plain `SELECT ... FROM information_schema.<view>` |
| Postgres-specific properties (e.g. table storage params, role attributes) | `pg_catalog` tables (`pg_class`, `pg_roles`, `pg_database`) |
| Properties split across schemas/catalogs | `JOIN` `information_schema` with `pg_catalog` |

Not-found is natural here too — an empty `WHERE` match returns zero rows,
no exception handling required.

### Simple (information_schema / pg_catalog)

```sql
SELECT json_build_object(
    'owner', r.rolname,
    'encoding', pg_encoding_to_char(d.encoding),
    'lc_collate', d.datcollate,
    'connection_limit', d.datconnlimit
) AS object_metadata
FROM pg_database d
JOIN pg_roles r ON r.oid = d.datdba
WHERE d.datname = '{{ name }}';
```

### Joined views with nested columns

```sql
SELECT json_build_object(
    'columns', json_agg(
        json_build_object(
            'name', c.column_name,
            'type', c.data_type,
            'nullable', c.is_nullable = 'YES'
        ) ORDER BY c.ordinal_position
    )
) AS object_metadata
FROM information_schema.columns c
WHERE c.table_schema = '{{ name.split(".")[0] }}'
  AND c.table_name = '{{ name.split(".")[1] }}'
GROUP BY c.table_schema, c.table_name;
```

### Multi-step (DO block, only when a single SELECT can't assemble the result)

Postgres `DO $$ ... $$` blocks are procedural but **cannot return a result
set** — they're for side effects only. If a resource genuinely needs
multi-step introspection, prefer a `WITH` CTE chain or a `LATERAL` join
over a `DO` block, since the state query must produce a `SELECT`ed row:

```sql
WITH base AS (
    SELECT oid, relname FROM pg_class WHERE relname = '{{ name }}'
),
opts AS (
    SELECT unnest(reloptions) AS opt FROM pg_class WHERE relname = '{{ name }}'
)
SELECT json_build_object(
    'name', base.relname,
    'fillfactor', (
        SELECT split_part(opt, '=', 2)::int FROM opts WHERE opt LIKE 'fillfactor=%'
    )
) AS object_metadata
FROM base;
```

## Engine notes

- Prefer `json_build_object(...)` over `row_to_json` for control over key
  names — the state JSON's keys must match the Jinja variables exactly, not
  the underlying column names.
- Use `NULLIF(x, '')` to normalize empty strings to `NULL`, same convention
  as Snowflake.
- Postgres identifiers are lower-cased by default unless quoted — don't
  `UPPER()` names the way Oracle needs; match the case as stored.
- `LEFT JOIN` when a property may not exist for every row (e.g. optional
  extensions or dependent objects).
