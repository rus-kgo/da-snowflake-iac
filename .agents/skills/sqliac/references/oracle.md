# Oracle Reference

Oracle has **no `CREATE OR ALTER`**. Every resource needs separate
CREATE / ALTER / DROP branches, and state comes from the data dictionary
views (`DBA_*`, `ALL_*`, `USER_*`) rather than a `SHOW`-style command.

## DDL command strategy by resource

| Strategy | Resources |
|----------|-----------|
| `CREATE` / `ALTER` (always separate — no idempotent form exists) | table, view, tablespace, user, role, sequence, index, synonym, procedure/function/package |
| `GRANT` / `REVOKE` | grant (roles and object privileges) |

## Template shape

```sql
{% set add = add | default({}) %}
{% set change = change | default({}) %}
{% set remove = remove | default({}) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE <RESOURCE> {{ name }}
    <REQUIRED_PROPS>
    {% if add.get('optional_prop') is not none %}
    {{ "OPTIONAL_PROP " ~ add.optional_prop | sql_escape }}
    {% endif %};

{% elif ddl_command.upper() == 'ALTER' %}
{% if change.get('optional_prop') is not none %}
ALTER <RESOURCE> {{ name }} <OPTIONAL_PROP_CLAUSE> {{ change.optional_prop | sql_escape }};
{% endif %}

{% elif ddl_command.upper() == 'DROP' %}
DROP <RESOURCE> {{ name }};
{% endif -%}
```

Oracle's `ALTER` clause names vary a lot per resource (`MODIFY`, `RENAME
TO`, `SET`, no single unifying verb like Snowflake's `SET`) — check the
resource's `ALTER <RESOURCE>` syntax page before writing the branch, and
emit one `ALTER` statement per changed property rather than trying to
combine them, since Oracle's grammar for combined alters is inconsistent
across resource types.

## Nested props example (table columns)

Oracle uses `MODIFY`/`ADD`/`DROP COLUMN`, each requiring its own statement:

```sql
{% set add_cols = add.get('columns', []) %}
{% set change_cols = change.get('columns', []) %}
{% set remove_cols = remove.get('columns', []) %}

{% if ddl_command.upper() == 'CREATE' %}
CREATE TABLE {{ name }} (
{% for column in add_cols %}
    {{ column.name }} {{ column.type }}{% if column.get('nullable') == 'N' %} NOT NULL{% endif %}{% if not loop.last %},{% endif %}
{% endfor %}
);

{% elif ddl_command.upper() == 'ALTER' %}
{% for column in add_cols %}
ALTER TABLE {{ name }} ADD ({{ column.name }} {{ column.type }});
{% endfor %}
{% for column in change_cols %}
{% if column.get('type') %}
ALTER TABLE {{ name }} MODIFY ({{ column.name }} {{ column.type }});
{% endif %}
{% endfor %}
{% for column in remove_cols %}
ALTER TABLE {{ name }} DROP COLUMN {{ column.name }};
{% endfor %}

{% elif ddl_command.upper() == 'DROP' %}
DROP TABLE {{ name }};
{% endif -%}
```

## Choosing the right introspection method

| Scenario | Technique |
|----------|-----------|
| Single dictionary view has everything | Direct `SELECT ... FROM DBA_<RESOURCE>S WHERE ... = UPPER('{{ name }}')` |
| Properties split across dictionary views | `JOIN` the views (e.g. `DBA_TABLES` + `DBA_TAB_COLUMNS`) |
| Properties only visible via `DBMS_METADATA.GET_DDL` | Parse the returned DDL text as a last resort — prefer dictionary views wherever they exist, since parsing DDL text is fragile |

Not-found handling is natural in Oracle: a `WHERE` clause that matches
nothing already returns zero rows — no exception block needed, unlike
Snowflake's `EXECUTE IMMEDIATE`/`EXCEPTION` pattern.

### Simple (single dictionary view)

```sql
SELECT JSON_OBJECT(
    'tablespace_name' VALUE t.tablespace_name,
    'logging'         VALUE t.logging,
    'block_size'      VALUE t.block_size
    RETURNING CLOB
) AS object_metadata
FROM dba_tablespaces t
WHERE t.tablespace_name = UPPER('{{ name }}');
```

### Joined views (table + columns)

```sql
SELECT JSON_OBJECT(
    'name' VALUE t.table_name,
    'columns' VALUE JSON_ARRAYAGG(
        JSON_OBJECT(
            'name' VALUE c.column_name,
            'type' VALUE c.data_type,
            'nullable' VALUE c.nullable
            ABSENT ON NULL
        )
        ORDER BY c.column_id
    )
    RETURNING CLOB
) AS object_metadata
FROM dba_tables t
JOIN dba_tab_columns c
  ON c.owner = t.owner AND c.table_name = t.table_name
WHERE t.table_name = UPPER('{{ name.split(".")[-1] }}')
GROUP BY t.table_name;
```

### Multi-step (PL/SQL anonymous block, when a single query can't assemble the JSON)

```sql
DECLARE
    v_json CLOB;
BEGIN
    SELECT JSON_OBJECT(
        'owner' VALUE owner,
        'status' VALUE status
        RETURNING CLOB
    )
    INTO v_json
    FROM dba_objects
    WHERE object_name = UPPER('{{ name }}')
      AND object_type = '<RESOURCE_TYPE>';

    DBMS_OUTPUT.PUT_LINE(v_json);
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        NULL; -- resource does not exist: emit nothing
END;
/
```

## Engine notes

- Oracle identifiers are case-insensitive and stored upper-case unless
  quoted — always `UPPER('{{ name }}')` in `WHERE` clauses to match.
- `JSON_OBJECT(... RETURNING CLOB)` is the standard JSON constructor
  (12.2+); use `ABSENT ON NULL` to drop null keys if the DDL template
  doesn't need to distinguish "null" from "absent".
- No native array-agg-into-JSON before 19c in some editions — check the
  target version before relying on `JSON_ARRAYAGG`.
- Handle not-found by relying on the `WHERE` clause, or `WHEN NO_DATA_FOUND`
  in PL/SQL blocks — never let an unhandled exception propagate.
