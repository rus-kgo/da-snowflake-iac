-- State query for Snowflake Storage Integration resource
-- Co-authored with CoCo

-- Uses DESCRIBE INTEGRATION to retrieve all properties that SHOW alone doesn't expose
EXECUTE IMMEDIATE $$
BEGIN
    DESCRIBE INTEGRATION {{ name }};

    LET rs RESULTSET := (
        WITH props AS (
            SELECT
                MAX(CASE WHEN "property" = 'STORAGE_PROVIDER' THEN "property_value" END) AS storage_provider,
                MAX(CASE WHEN "property" = 'ENABLED' THEN "property_value" END) AS enabled,
                MAX(CASE WHEN "property" = 'STORAGE_ALLOWED_LOCATIONS' THEN "property_value" END) AS storage_allowed_locations,
                MAX(CASE WHEN "property" = 'STORAGE_BLOCKED_LOCATIONS' THEN "property_value" END) AS storage_blocked_locations,
                MAX(CASE WHEN "property" = 'STORAGE_AWS_ROLE_ARN' THEN "property_value" END) AS storage_aws_role_arn,
                MAX(CASE WHEN "property" = 'AZURE_TENANT_ID' THEN "property_value" END) AS azure_tenant_id,
                MAX(CASE WHEN "property" = 'COMMENT' THEN "property_value" END) AS comment
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        )
        SELECT object_construct_keep_null(
            'storage_provider', props.storage_provider,
            'enabled', props.enabled = 'true',
            'storage_allowed_locations', props.storage_allowed_locations,
            'storage_blocked_locations', NULLIF(props.storage_blocked_locations, ''),
            'storage_aws_role_arn', NULLIF(props.storage_aws_role_arn, ''),
            'azure_tenant_id', NULLIF(props.azure_tenant_id, ''),
            'comment', NULLIF(props.comment, '')
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
