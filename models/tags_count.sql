WITH Tags AS (
    SELECT
        Video_ID
      , Trending_Date
      , Tags
    FROM {{ ref('video_info') }}
), stg_tags_list AS (
    SELECT Video_ID
        , Trending_Date
        , split_tags.index     AS Tag_Index
        , split_tags.value     AS Tag_Value
    FROM Tags,
    LATERAL split_to_table(Tags.Tags, ',') AS split_tags
),
tags_count AS (
    SELECT DISTINCT
        LISTAGG(video_id, ', ') WITHIN GROUP (ORDER BY trending_date DESC) OVER (PARTITION BY stg_tags_list.Tag_Value)                 AS videos_id_list
        , YEAR(trending_date)                   AS year
        , QUARTER(trending_date)                AS quarter
        , MONTH(trending_date)                  AS month
        , stg_tags_list.Tag_Value                                                                                             AS tag
        , COUNT(*)                OVER (PARTITION BY year, quarter, month, tag)                                                                     AS tag_count
        , MAX(stg_tags_list.trending_date) OVER (PARTITION BY tag)                                                            AS last_trend_date
    FROM stg_tags_list
)
SELECT *
FROM tags_count
WHERE tag !LIKE '[none]'
ORDER BY year, quarter, month, tag_count DESC
