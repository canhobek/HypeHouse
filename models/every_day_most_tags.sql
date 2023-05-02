/*
WITH tags AS (
    SELECT *
    FROM {{ref('stg_tags_list')}}
)
SELECT COUNT(DISTINCT tag_value)            AS tag_count
    , trending_date 
FROM tags
WHERE tag_value !LIKE '[none]'
    AND tag_count = (SELECT tag_count FROM tags LIMIT 5 ORDER BY tag_count DESC) 
GROUP BY trending_date
ORDER BY trending_date DESC
*/