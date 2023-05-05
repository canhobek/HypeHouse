WITH Tags AS (
    SELECT
        Video_ID
      , Trending_Date
      , Tags
    FROM {{ ref('video_info') }}
), WITH stg_tags_list AS (
    SELECT Video_ID
        , Trending_Date
        , split_tags.index     AS Tag_Index
        , split_tags.value     AS Tag_Value
    FROM Tags,
    LATERAL split_to_table(Tags.Tags, ',') AS split_tags
), 
--WITH tags AS (
--    SELECT *
--    FROM {{ref('stg_tags_list')}}
--),
tags_count AS (
    SELECT DISTINCT
        LISTAGG(video_id, ', ') WITHIN GROUP (ORDER BY trending_date DESC) OVER (PARTITION BY tags.Tag_Value)                 AS videos_id_list
        , stg_tags_list.Tag_Value                                                                                             AS tag
        , COUNT(*)                OVER (PARTITION BY tag)                                                                     AS tag_count
        , MAX(stg_tags_list.trending_date) OVER (PARTITION BY tag)                                                            AS last_trend_date
    FROM stg_tags_list
)
SELECT *
FROM tags_count
WHERE tag !LIKE '[none]'
ORDER BY tag_count DESC
