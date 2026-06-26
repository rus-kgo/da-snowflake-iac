SELECT object_construct(
    'name', table_name,
    'database', table_catalog,
    'schema', table_schema,
    'comment', comment,
    'columns',  array_agg(
            object_construct(
                'name',"COLUMN_NAME",
                'type', "DATA_TYPE",
                'nullable', "IS_NULLABLE" = 'YES'
            )) within group (order by "ORDINAL_POSITION")
) AS table_metadata
FROM snowflake.information_schema.columns
WHERE table_name = '{{ name }}'
  AND table_schema = '{{ schema }}'
  AND table_catalog = '{{ database }}'
GROUP BY table_catalog, table_schema, table_name, comment
LIMIT 1;
