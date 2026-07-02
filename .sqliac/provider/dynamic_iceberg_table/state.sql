-- State query for Snowflake Dynamic Iceberg Table resource
-- Co-authored with CoCo
{% set name_parts = name.split('.') %}

-- Combines SHOW DYNAMIC TABLES with SHOW ICEBERG TABLES to get iceberg-specific properties
EXECUTE IMMEDIATE $$
BEGIN
    SHOW DYNAMIC TABLES LIKE '{{ name_parts[2] }}' IN SCHEMA {{ name_parts[0] }}.{{ name_parts[1] }};

    LET v_target_lag VARCHAR := '';
    LET v_warehouse VARCHAR := '';
    LET v_query VARCHAR := '';
    LET v_comment VARCHAR := '';

    LET c1 CURSOR FOR
        SELECT "target_lag", "warehouse", "text", NULLIF("comment", '')
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "name" = '{{ name_parts[2] }}'
          AND "is_iceberg" = 'Y';

    OPEN c1;
    FETCH c1 INTO v_target_lag, v_warehouse, v_query, v_comment;
    CLOSE c1;

    -- Get iceberg-specific metadata (external_volume, catalog, base_location)
    SHOW ICEBERG TABLES LIKE '{{ name_parts[2] }}' IN SCHEMA {{ name_parts[0] }}.{{ name_parts[1] }};

    LET rs RESULTSET := (
        SELECT object_construct_keep_null(
            'target_lag', :v_target_lag,
            'warehouse', :v_warehouse,
            'external_volume', "external_volume_name",
            'catalog', "catalog_name",
            'base_location', "base_location",
            'query', :v_query,
            'comment', :v_comment
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
