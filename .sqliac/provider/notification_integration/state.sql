-- State query for Snowflake Notification Integration resource
-- Co-authored with CoCo

-- Uses DESCRIBE INTEGRATION to get provider-specific properties not exposed by SHOW
EXECUTE IMMEDIATE $$
BEGIN
    DESCRIBE INTEGRATION {{ name }};

    LET rs RESULTSET := (
        WITH props AS (
            SELECT
                MAX(CASE WHEN "property" = 'ENABLED' THEN "property_value" END) AS enabled,
                MAX(CASE WHEN "property" = 'NOTIFICATION_PROVIDER' THEN "property_value" END) AS notification_provider,
                MAX(CASE WHEN "property" = 'DIRECTION' THEN "property_value" END) AS direction,
                MAX(CASE WHEN "property" = 'AWS_SQS_ARN' THEN "property_value" END) AS aws_sqs_arn,
                MAX(CASE WHEN "property" = 'AWS_SQS_ROLE_ARN' THEN "property_value" END) AS aws_sqs_role_arn,
                MAX(CASE WHEN "property" = 'AZURE_STORAGE_QUEUE_PRIMARY_URI' THEN "property_value" END) AS azure_storage_queue_primary_uri,
                MAX(CASE WHEN "property" = 'AZURE_TENANT_ID' THEN "property_value" END) AS azure_tenant_id,
                MAX(CASE WHEN "property" = 'GCP_PUBSUB_SUBSCRIPTION_NAME' THEN "property_value" END) AS gcp_pubsub_subscription_name,
                MAX(CASE WHEN "property" = 'COMMENT' THEN "property_value" END) AS comment
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        )
        SELECT object_construct_keep_null(
            'enabled', props.enabled = 'true',
            'notification_provider', props.notification_provider,
            'direction', NULLIF(props.direction, ''),
            'aws_sqs_arn', NULLIF(props.aws_sqs_arn, ''),
            'aws_sqs_role_arn', NULLIF(props.aws_sqs_role_arn, ''),
            'azure_storage_queue_primary_uri', NULLIF(props.azure_storage_queue_primary_uri, ''),
            'azure_tenant_id', NULLIF(props.azure_tenant_id, ''),
            'gcp_pubsub_subscription_name', NULLIF(props.gcp_pubsub_subscription_name, ''),
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
