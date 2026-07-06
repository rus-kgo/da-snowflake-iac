{% set name_parts = name.split('.') %}

SELECT object_construct_keep_null(
    'columns', array_agg(object_construct_keep_null(
        'name', "COLUMN_NAME",
        'type', CASE WHEN DATA_TYPE = 'TEXT' THEN 'VARCHAR'
                ELSE DATA_TYPE END,
        'nullable', "IS_NULLABLE",
        'comment', comment
        )) within GROUP (ORDER BY "ORDINAL_POSITION")
    ) AS table_metadata
FROM {{ name_parts[0] }}.information_schema.columns
WHERE TABLE_NAME = '{{ name_parts[2] }}'
  AND table_schema = '{{ name_parts[1] }}'
GROUP BY table_catalog,
         table_schema,
         TABLE_NAME
LIMIT 1;

   BEGIN
      LET res RESULTSET := (
        SELECT object_construct_keep_null(
            'transient', is_transient = 'YES',
            'managed_access', is_managed_access = 'YES',
            'data_retention_time_in_days', retention_time,
            'comment', COMMENT
        ) AS object_metadata
        FROM RUSKGO_TEST.information_schema.schemata
        WHERE SCHEMA_NAME = 'MODELING_SCHEMA'
        LIMIT 1
      );
      RETURN TABLE(res);
    EXCEPTION
      WHEN OTHER THEN
        RETURN TABLE(SELECT NULL::VARIANT AS object_metadata WHERE 1=0);
    END;
