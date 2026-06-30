{% set name_parts = name.split('.') %}

SELECT object_construct_keep_null(
    'transient', is_transient = 'YES',
    'managed_access', is_managed_access = 'YES',
    'data_retention_time_in_days', retention_time,
    'comment', comment
) as object_metadata
FROM {{name_parts[0]}}.information_schema.schemata
WHERE schema_name = '{{name_parts[1]}}'
LIMIT 1
