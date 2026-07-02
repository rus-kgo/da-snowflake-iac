-- State query for Snowflake Role resource
-- Co-authored with CoCo

EXECUTE IMMEDIATE $$
BEGIN
    SHOW ROLES LIKE '{{ name }}';

    LET rs RESULTSET := (
        SELECT object_construct_keep_null(
            'comment', NULLIF("comment", '')
        ) AS object_metadata
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "name" = '{{ name }}'
        LIMIT 1
    );

    RETURN TABLE(rs);

EXCEPTION
    WHEN OTHER THEN
        LET rs_empty RESULTSET := (SELECT NULL AS object_metadata WHERE 1=0);
        RETURN TABLE(rs_empty);
END;
$$;
