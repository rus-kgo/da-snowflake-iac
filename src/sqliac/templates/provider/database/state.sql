SELECT object_construct(
    'name', database_name,
    'owner', database_owner,
    'transient', type = 'TRANSIENT',
    'data_retention_time_in_days', retention_time,
    'comment', comment,
    'created_on', created
) as object_metadata
FROM information_schema.databases
WHERE database_name = '{{ name }}'
LIMIT 1
