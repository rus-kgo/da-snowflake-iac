-- State query for Snowflake Stream resource
-- Co-authored with CoCo
{% set name_parts = name.split('.') %}

EXECUTE IMMEDIATE $$
BEGIN
    SHOW STREAMS LIKE '{{ name_parts[2] }}' IN SCHEMA {{ name_parts[0] }}.{{ name_parts[1] }};

    LET rs RESULTSET := (
        SELECT object_construct_keep_null(
            'source_type', "source_type",
            'source_name', "table_name",
            'append_only', "mode" = 'APPEND_ONLY',
            'show_initial_rows', "invalid_reason" IS NULL AND "comment" LIKE '%SHOW_INITIAL_ROWS%',
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
