EXECUTE IMMEDIATE $$
BEGIN
    SHOW PARAMETERS IN DATABASE IDENTIFIER('{{ name }}');

    -- We use a RESULTSET to capture the final query to return it
    LET rs RESULTSET := (
        WITH params AS (
            SELECT
                MAX(CASE WHEN "key" = 'MAX_DATA_EXTENSION_TIME_IN_DAYS' THEN "value" END) AS max_data_extension_time_in_days,
                MAX(CASE WHEN "key" = 'DEFAULT_DDL_COLLATION' THEN "value" END) AS default_ddl_collation,
                MAX(CASE WHEN "key" = 'LOG_LEVEL' THEN "value" END) AS log_level
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        )
        SELECT object_construct_keep_null(
            'transient', d.type = 'TRANSIENT',
            'data_retention_time_in_days', d.retention_time,
            'max_data_extension_time_in_days', p.max_data_extension_time_in_days::int,
            'default_ddl_collation', p.default_ddl_collation,
            'log_level', p.log_level
        ) AS object_metadata
        FROM snowflake.information_schema.databases d
        CROSS JOIN params p
        WHERE d.database_name = '{{ name }}'
        LIMIT 1
    );

    RETURN TABLE(rs);

EXCEPTION
    WHEN OTHER THEN
        LET rs_empty RESULTSET := (SELECT NULL AS object_metadata WHERE 1=0);
        RETURN TABLE(rs_empty);
END;
$$;
