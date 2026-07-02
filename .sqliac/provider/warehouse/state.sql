-- State query for Snowflake Warehouse resource
-- Co-authored with CoCo

EXECUTE IMMEDIATE $$
BEGIN
    SHOW WAREHOUSES LIKE '{{ name }}';

    LET rs RESULTSET := (
        SELECT object_construct_keep_null(
            'warehouse_size', "size",
            'auto_suspend', "auto_suspend"::int,
            'auto_resume', "auto_resume" = 'true',
            'min_cluster_count', "min_cluster_count"::int,
            'max_cluster_count', "max_cluster_count"::int,
            'scaling_policy', "scaling_policy",
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
