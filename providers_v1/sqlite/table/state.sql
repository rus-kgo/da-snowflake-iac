WITH table_info AS (
    SELECT 
        m.name as table_name,
        m.sql as table_definition,
        json_group_array(
            json_object(
                'name', p.name,
                'type', p.type,
                'nullable', CASE WHEN p."notnull" = 0 THEN 1 ELSE 0 END
            )
        ) as columns
    FROM sqlite_master m
    LEFT JOIN pragma_table_info('{{ name }}') p
    WHERE m.type = 'table' 
    AND m.name = '{{ name }}'
    GROUP BY m.name
)
SELECT json_object(
    'name', table_name,
    'database', 'main',
    'schema', 'main',
    'columns', json(columns)
) as table_metadata
FROM table_info;
