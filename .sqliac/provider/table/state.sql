SELECT object_construct(
    'name', table_name,
    'database', table_catalog,
    'schema', table_schema,
    'comment', comment,
    'columns', array_agg(
        object_construct(
            'name', column_name,
            'type', data_type,
            'nullable', is_nullable = 'YES'
        )
        ORDER BY ordinal_position
    )
) AS table_metadata
FROM information_schema.columns
WHERE table_name = '{{ name }}'
  AND table_schema = '{{ schema }}'
  AND table_catalog = '{{ database }}'
GROUP BY table_catalog, table_schema, table_name, comment
LIMIT 1;
