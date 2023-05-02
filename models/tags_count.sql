WITH tags AS (
    SELECT *
    FROM {{ref('stg_tags_list')}}
),
tags_count AS (
    SELECT DISTINCT
        LISTAGG(video_id, ', ') WITHIN GROUP (ORDER BY trending_date DESC) OVER (PARTITION BY tags.Tag_Value)                 AS videos_id_list
        , tags.Tag_Value                                                                                                      AS tag
        , COUNT(*)                OVER (PARTITION BY tag)                                                                     AS tag_count
        --, MIN(Tags.Tag_Index)     OVER (PARTITION BY Tag)               AS Highest_Order
        --, MAX(Tags.Tag_Index)     OVER (PARTITION BY Tag)               AS Lowest_Order
        , MAX(tags.trending_date) OVER (PARTITION BY tag)                                                                     AS last_trend_date
    FROM tags
)
SELECT *
FROM tags_count
WHERE --AND tag_count > 1
    tag !LIKE '[none]'
ORDER BY tag_count DESC--, Highest_Order, Lowest_Order
