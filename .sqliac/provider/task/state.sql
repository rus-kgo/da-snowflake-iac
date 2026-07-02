-- State query for Snowflake Task resource
-- Co-authored with CoCo
{% set name_parts = name.split('.') %}

EXECUTE IMMEDIATE $$
BEGIN
    SHOW TASKS LIKE '{{ name_parts[2] }}' IN SCHEMA {{ name_parts[0] }}.{{ name_parts[1] }};

    LET rs RESULTSET := (
        SELECT object_construct_keep_null(
            'warehouse', NULLIF("warehouse", ''),
            'schedule', NULLIF("schedule", ''),
            'after', NULLIF("predecessors", '[]'),
            'when', NULLIF("condition", ''),
            'sql_statement', "definition",
            'comment', NULLIF("comment", '')
        ) AS object_metadata
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "name" = '{{ name_parts[2] }}'
        LIMIT 1
    );

    RETURN TABLE(rs);

EXCEPTION
    WHEN OTHER THEN
        LET rs_empty RESULTSET := (SELECT NULL AS object_metadata WHERE 1=0);
        RETURN TABLE(rs_empty);
END;
$$;
