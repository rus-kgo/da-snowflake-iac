-- State query for Snowflake Security Integration resource
-- Co-authored with CoCo

-- Uses DESCRIBE INTEGRATION to get type-specific properties (SAML2, OAuth)
EXECUTE IMMEDIATE $$
BEGIN
    DESCRIBE INTEGRATION {{ name }};

    LET rs RESULTSET := (
        WITH props AS (
            SELECT
                MAX(CASE WHEN "property" = 'TYPE' THEN "property_value" END) AS type,
                MAX(CASE WHEN "property" = 'ENABLED' THEN "property_value" END) AS enabled,
                MAX(CASE WHEN "property" = 'SAML2_ISSUER' THEN "property_value" END) AS saml2_issuer,
                MAX(CASE WHEN "property" = 'SAML2_SSO_URL' THEN "property_value" END) AS saml2_sso_url,
                MAX(CASE WHEN "property" = 'SAML2_PROVIDER' THEN "property_value" END) AS saml2_provider,
                MAX(CASE WHEN "property" = 'SAML2_X509_CERT' THEN "property_value" END) AS saml2_x509_cert,
                MAX(CASE WHEN "property" = 'OAUTH_CLIENT' THEN "property_value" END) AS oauth_client,
                MAX(CASE WHEN "property" = 'COMMENT' THEN "property_value" END) AS comment
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        )
        SELECT object_construct_keep_null(
            'type', props.type,
            'enabled', props.enabled = 'true',
            'saml2_issuer', NULLIF(props.saml2_issuer, ''),
            'saml2_sso_url', NULLIF(props.saml2_sso_url, ''),
            'saml2_provider', NULLIF(props.saml2_provider, ''),
            'saml2_x509_cert', NULLIF(props.saml2_x509_cert, ''),
            'oauth_client', NULLIF(props.oauth_client, ''),
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
