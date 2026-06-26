SELECT object_construct(
    'name', database_name,
    'owner', database_owner,
    'transient', type = 'TRANSIENT',
    'data_retention_time_in_days', retention_time
) as object_metadata
FROM snowflake.information_schema.databases
WHERE database_name = '{{ name }}'
LIMIT 1
