# Databricks Reference

Databricks (Unity Catalog) uses the same three-level namespace as Snowflake
(`catalog.schema.object`) and supports `CREATE OR REPLACE` for most objects,
making it closer to Snowflake's model than Oracle's or Postgres's.

## DDL command strategy by resource

| Strategy | Resources |
|----------|-----------|
| `CREATE OR REPLACE` | table*, view, function |
| `CREATE` / `ALTER` (separate) | catalog, schema, volume, warehouse (SQL warehouse), external location, storage credential |
| `GRANT` / `REVOKE` | grant (Unity Catalog privileges) |

\* `CREATE OR REPLACE TABLE` **drops and recreates** the table (data loss)
unless it's a Delta table using `CREATE OR REPLACE TABLE ... AS SELECT`
against the same source. For column-level changes to an existing managed
table, prefer explicit `ALTER TABLE ... ADD/DROP/ALTER COLUMN` /
`SET TBLPROPERTIES` instead of `CREATE OR REPLACE` to avoid rewriting data.

## Template shape (CREATE OR REPLACE resources)

```sql
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE OR REPLACE' %}
CREATE OR REPLACE VIEW {{ name }}
  {% if add.get('comment') or change.get('comment') %}
  COMMENT '{{ add.get('comment') or change.get('comment') }}'
  {% endif %}
  AS
    {{ add.get('query') or change.get('query') }};

{% elif ddl_command.upper() == 'DROP' %}
DROP VIEW IF EXISTS {{ name }};
{% endif -%}
```

## Template shape (separate CREATE/ALTER resources, e.g. schema)

```sql
{% set add = add | default({}) %}
{% set change = change | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE SCHEMA IF NOT EXISTS {{ name }}
  {% if add.get('comment') %}COMMENT '{{ add.comment | sql_escape }}'{% endif %}
  {% if add.get('managed_location') %}MANAGED LOCATION '{{ add.managed_location }}'{% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
{% if change.get('comment') is not none %}
ALTER SCHEMA {{ name }} SET COMMENT '{{ change.comment | sql_escape }}';
{% endif %}
{% if change.get('owner') %}
ALTER SCHEMA {{ name }} OWNER TO `{{ change.owner }}`;
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP SCHEMA IF EXISTS {{ name }} CASCADE;
{% endif -%}
```

## Nested props example (table columns via TBLPROPERTIES / ALTER COLUMN)

```sql
{% set add_cols = add.get('columns', []) %}
{% set change_cols = change.get('columns', []) %}
{% set remove_cols = remove.get('columns', []) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE TABLE {{ name }} (
{% for column in add_cols %}
    {{ column.name }} {{ column.type }}{% if not column.get('nullable', true) %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}
{% endfor %}
) USING DELTA;

{% elif ddl_command.upper() == 'ALTER' %}
{% for column in add_cols %}
ALTER TABLE {{ name }} ADD COLUMN {{ column.name }} {{ column.type }};
{% endfor %}
{% for column in change_cols %}
{% if column.get('type') %}
ALTER TABLE {{ name }} ALTER COLUMN {{ column.name }} TYPE {{ column.type }};
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
| Unity Catalog `information_schema` has everything | `SELECT ... FROM system.information_schema.<view>` or `<catalog>.information_schema.<view>` |
| Properties only visible via `DESCRIBE` | `DESCRIBE TABLE EXTENDED` / `DESCRIBE DETAIL` / `SHOW TBLPROPERTIES`, parsed via `to_json`/`named_struct` |
| Grants | `SHOW GRANTS ON <object>` |

Databricks SQL has no anonymous-block/procedural equivalent to Snowflake's
`EXECUTE IMMEDIATE` — every state query must be a single `SELECT`. When a
resource needs data from more than one `DESCRIBE`/`SHOW` command, that
command's output isn't queryable as a table the way
`RESULT_SCAN(LAST_QUERY_ID())` is in Snowflake, so **prefer
`information_schema` joins over `DESCRIBE`/`SHOW` whenever the property is
exposed there** — it keeps the state query a plain `SELECT`.

### Simple (information_schema)

```sql
SELECT to_json(named_struct(
    'comment', comment,
    'owner', schema_owner
)) AS object_metadata
FROM system.information_schema.schemata
WHERE catalog_name = '{{ name.split(".")[0] }}'
  AND schema_name = '{{ name.split(".")[1] }}';
```

### Joined views with nested columns

```sql
SELECT to_json(named_struct(
    'columns', collect_list(named_struct(
        'name', column_name,
        'type', data_type,
        'nullable', is_nullable = 'YES'
    ))
)) AS object_metadata
FROM system.information_schema.columns
WHERE table_catalog = '{{ name.split(".")[0] }}'
  AND table_schema = '{{ name.split(".")[1] }}'
  AND table_name = '{{ name.split(".")[2] }}'
GROUP BY table_catalog, table_schema, table_name;
```

### When DESCRIBE/SHOW output must be captured (host-language workaround)

If a property genuinely only appears in `DESCRIBE TABLE EXTENDED` or
`SHOW TBLPROPERTIES` output and not in `information_schema`, this can't be
folded into one state query in pure Databricks SQL. Options, in order of
preference:
1. Check whether the property was added to `information_schema` in a newer
   Databricks runtime before working around it.
2. Have the calling adapter run the `DESCRIBE`/`SHOW` command separately
   and merge its result into the JSON in the host language (equivalent to
   Snowflake's cursor-capture pattern, but done outside SQL since Databricks
   SQL has no cursor/variable syntax).
3. As a last resort, `SELECT` a constant `NULL` for that key and flag it as
   a known gap in the resource's `state.sql` header comment.

## Engine notes

- Three-level naming (`catalog.schema.object`) mirrors Snowflake — reuse
  `{% set name_parts = name.split('.') %}` for schema-scoped resources.
- Delta table properties live in `TBLPROPERTIES`; prefer
  `information_schema.tables`/`columns` over parsing `SHOW TBLPROPERTIES`
  output when possible.
- Grants: `SHOW GRANTS ON <securable_type> <name>` returns a queryable
  result set directly (no `RESULT_SCAN` needed) — can be selected from
  directly in some Databricks SQL versions, but confirm on the target
  runtime since this has changed across versions.
