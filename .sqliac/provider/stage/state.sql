-- State query for Snowflake Stage resource
-- Co-authored with CoCo
{% set name_parts = name.split('.') %}

-- Uses DESCRIBE STAGE to get encryption and directory settings not exposed by SHOW
EXECUTE IMMEDIATE $$
BEGIN
    SHOW STAGES LIKE '{{ name_parts[2] }}' IN SCHEMA {{ name_parts[0] }}.{{ name_parts[1] }};

    LET v_url VARCHAR := '';
    LET v_storage_integration VARCHAR := '';
    LET v_comment VARCHAR := '';

    LET c1 CURSOR FOR
        SELECT NULLIF("url", '') AS url,
               NULLIF("storage_integration", '') AS storage_integration,
               NULLIF("comment", '') AS comment
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE "name" = '{{ name_parts[2] }}';

    OPEN c1;
    FETCH c1 INTO v_url, v_storage_integration, v_comment;
    CLOSE c1;

    -- DESCRIBE STAGE returns stage properties including encryption and directory
    DESCRIBE STAGE {{ name_parts[0] }}.{{ name_parts[1] }}.{{ name_parts[2] }};

    LET rs RESULTSET := (
        WITH props AS (
            SELECT
                MAX(CASE WHEN "parent_property" = 'STAGE_ENCRYPTION' AND "property" = 'TYPE' THEN "property_value" END) AS encryption_type,
                MAX(CASE WHEN "parent_property" = 'DIRECTORY' AND "property" = 'ENABLE' THEN "property_value" END) AS directory_enabled
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        )
        SELECT object_construct_keep_null(
            'url', :v_url,
            'storage_integration', :v_storage_integration,
            'encryption_type', NULLIF(props.encryption_type, ''),
            'directory_enabled', props.directory_enabled = 'true',
            'comment', :v_comment
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
