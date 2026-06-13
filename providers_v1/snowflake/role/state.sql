SELECT object_construct(
    'name', name,
    'owner', owner,
    'comment', comment,
    'created_on', created_on
) as object_metadata
FROM snowflake.account_usage.roles
WHERE name = '{{ name }}'
AND deleted_on IS NULL
LIMIT 1
