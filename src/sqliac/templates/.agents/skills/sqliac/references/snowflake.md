# Snowflake Reference

## DDL command strategy by resource

| Strategy | Resources |
|----------|-----------|
| `CREATE OR ALTER` | database, schema, table*, view, stage, role, database role, warehouse†, alert†, event table, notification/security/storage integration† |
| `CREATE` / `ALTER` (separate) | user, procedure, task, stream, dynamic table, dynamic iceberg table |
| `GRANT` / `REVOKE` | grant |

\* `CREATE OR ALTER TABLE` doesn't support column-level ADD/DROP/RENAME in
one shot — use separate `ALTER TABLE ... ADD/DROP/RENAME COLUMN` statements
per column even though the top-level command is idempotent create-or-alter.
† Several of these ship in this repo using separate CREATE/ALTER for
historical reasons even though Snowflake now supports CREATE OR ALTER for
them (Nov 2024+) — CREATE OR ALTER is preferred for anything new.

## Single create/alter statement example

```sql
{% set add = add | default({}) %}
{% set change = change | default({}) %}

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

## Separate create/alter statements example

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

## Nested props example (e.g. table columns)

```sql
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
{% for item in add_items %}
ALTER <RESOURCE> {{ name }} ADD PROP {{ item.prop }};
{% endfor %}
{% for item in remove_items %}
ALTER <RESOURCE> {{ name }} DROP PROP {{ item.prop }};
{% endfor %}

{% elif ddl_command.upper() == 'DROP' %}
DROP <RESOURCE> {{ name }};
{% endif -%}
```

## Choosing the right introspection method

| Scenario | Technique |
|----------|-----------|
| All properties available from a single `SHOW` command | `SHOW ... ` + filter via `RESULT_SCAN(LAST_QUERY_ID())` |
| Some properties only via `DESCRIBE` | `DESCRIBE ...` and pivot rows into columns via `CASE WHEN` |
| Properties split across multiple commands | Chain commands in a procedural block, capture the first into a variable via a cursor, then run the second |

Type normalization example (Snowflake returns type aliases, e.g. `TEXT` for
`VARCHAR`):

```sql
SELECT object_construct_keep_null(
    'type', CASE
        WHEN DATA_TYPE = 'TEXT' THEN 'VARCHAR'
        ELSE DATA_TYPE
    END,
    'nullable', (IS_NULLABLE = 'YES')
) ...
```

### Simple (SHOW exposes everything)

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
        WHERE "name" = '{{ name_parts[1] }}'
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

### DESCRIBE pivot (integrations, stages — key/value property output)

```sql
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

### Multi-command (combine two SHOW commands via a cursor variable)

```sql
EXECUTE IMMEDIATE $$
BEGIN
    SHOW <RESOURCE_A> LIKE '...' IN ...;
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

## Engine notes

- Use `EXECUTE IMMEDIATE $$ BEGIN ... END; $$` for any multi-statement
  introspection logic.
- `TABLE(RESULT_SCAN(LAST_QUERY_ID()))` captures the prior SHOW/DESCRIBE output.
- The exception handler is what makes not-found return zero rows instead of
  erroring: `SELECT NULL AS object_metadata WHERE 1=0`.
- Schema-scoped objects: `{% set name_parts = name.split('.') %}`, since
  `name` is always the fully-qualified `DATABASE.SCHEMA.OBJECT`.
- Booleans from SHOW output come back as the strings `'true'`/`'false'` —
  compare explicitly: `"is_secure" = 'true'`.
