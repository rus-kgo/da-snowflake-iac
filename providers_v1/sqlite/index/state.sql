WITH index_info AS (
    SELECT
        m.name AS index_name,
        m.tbl_name AS table_name,
        il."unique" AS is_unique,
        (
            SELECT json_group_array(
                json_object(
                    'name', ii.name
                )
            )
            FROM pragma_index_info(m.name) ii
            ORDER BY ii.seqno
        ) AS columns
    FROM sqlite_master m
    JOIN pragma_index_list(m.tbl_name) il
        ON il.name = m.name
    WHERE m.type = 'index'
      AND m.name = '{{ name }}'
)
SELECT json_object(
    'name', index_name,
    'table', table_name,
    'database', 'main',
    'schema', 'main',
    'unique', is_unique,
    'columns', json(columns)
) AS index_metadata
FROM index_info;
