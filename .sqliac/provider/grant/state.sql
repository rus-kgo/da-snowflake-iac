-- State query for Snowflake Grant resource
-- Co-authored with CoCo

-- Checks if the specific grant exists; returns empty if not found
EXECUTE IMMEDIATE $$
BEGIN
    SHOW GRANTS ON {{ on_type }} {{ on_name }};

    LET rs RESULTSET := (
        SELECT object_construct_keep_null(
            'privilege', "privilege",
            'on_type', "granted_on",
            'on_name', "name",
            'to_role', "grantee_name",
            'with_grant_option', "grant_option" = 'true'
        ) AS object_metadata
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "privilege" = '{{ privilege }}'
          AND "grantee_name" = '{{ to_role }}'
        LIMIT 1
    );

    RETURN TABLE(rs);

EXCEPTION
    WHEN OTHER THEN
        LET rs_empty RESULTSET := (SELECT NULL AS object_metadata WHERE 1=0);
        RETURN TABLE(rs_empty);
END;
$$;
