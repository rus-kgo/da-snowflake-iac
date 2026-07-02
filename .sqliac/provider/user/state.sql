-- State query for Snowflake User resource
-- Co-authored with CoCo

EXECUTE IMMEDIATE $$
BEGIN
    SHOW USERS LIKE '{{ name }}';

    LET rs RESULTSET := (
        SELECT object_construct_keep_null(
            'login_name', "login_name",
            'display_name', "display_name",
            'email', NULLIF("email", ''),
            'default_role', NULLIF("default_role", ''),
            'default_warehouse', NULLIF("default_warehouse", ''),
            'default_namespace', NULLIF("default_namespace", ''),
            'disabled', "disabled" = 'true',
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
