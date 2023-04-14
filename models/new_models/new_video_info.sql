with new_video_info_aws as (
    SELECT DISTINCT dt.value:index::number                                                       AS index 
        , dt.value:categoryId_for_client_HyPeHoUsE::number                              AS category_id
        , dt.value:channelId_for_client_HyPeHoUsE::varchar                              AS channel_id
        , dt.value:channelTitle_for_client_HyPeHoUsE::varchar                           AS channel_title
        , dt.value:comments_disabled_for_client_HyPeHoUsE::boolean                      AS comments_disabled
        , dt.value:description_for_client_HyPeHoUsE::varchar                            AS description
        , dt.value:dislikes_for_client_HyPeHoUsE::varchar                               AS dislikes
        , CONVERT_TIMEZONE('Etc/GMT', 'Europe/Paris', dt.value:publishedAt_for_client_HyPeHoUsE::datetime::timestamp_ntz)     AS published_at
        , dt.value:ratings_disabled_for_client_HyPeHoUsE::boolean                       AS ratings_disabled
        , REPLACE(dt.value:tags_for_client_HyPeHoUsE::varchar, '|', ',')                AS tags
        , dt.value:thumbnail_link_for_client_HyPeHoUsE::varchar                         AS thumbnail_link
        , dt.value:title_for_client_HyPeHoUsE::varchar                                  AS title
        , TO_DATE(dt.value:trending_date_for_client_HyPeHoUsE::varchar, 'YY.DD.MM')     AS trending_date
        , dt.value:video_id_for_client_HyPeHoUsE::varchar                               AS video_id
    FROM {{ source('new_raw_data', 'new_video_info_aws') }}
    , LATERAL FLATTEN(INPUT => col1:data) as dt
),
video_info_aws AS (    
    SELECT null                                             AS index
        , col1:categoryId_for_client_HyPeHoUsE::number                                                                     AS category_id
        , col1:channelId_for_client_HyPeHoUsE::varchar                                                                          AS channel_id
        , col1:"channelTitle                                      _for_client_HyPeHoUsE"::varchar                               AS channel_title
        , col1:comments_disabled_for_client_HyPeHoUsE::boolean                                                                  AS comments_disabled
        , col1:description_for_client_HyPeHoUsE::varchar                                                                        AS description
        , col1:dislikes_for_client_HyPeHoUsE::numeric                                                                           AS dislikes
        --, col1:likes_for_client_HyPeHoUsE::numeric                                                                              AS likes
        , CONVERT_TIMEZONE('Etc/GMT', 'Europe/Paris', col1:publishedAt_for_client_HyPeHoUsE::datetime::timestamp_ntz)           AS published_at
        , col1:ratings_disabled_for_client_HyPeHoUsE::boolean                                                                   AS ratings_disabled
        , REPLACE(col1:tags_for_client_HyPeHoUsE::varchar, '|', ',')                                                            AS tags
        , col1:thumbnail_link_for_client_HyPeHoUsE::varchar                                                                     AS thumbnail_link
        , col1:title_for_client_HyPeHoUsE::varchar                                                                              AS title
        , TO_DATE(col1:trending_date_for_client_HyPeHoUsE::varchar, 'YY.DD.MM')                                                 AS trending_date
        , col1:video_id_for_client_HyPeHoUsE::varchar                                                                           AS video_id
    FROM {{ source('public', 'video_info_aws') }}
    WHERE video_id NOT LIKE 'video_id'
),
youtube_category as (
    SELECT category_id as youtube_category_id
        , category_name
    FROM raw_db.dbt_hh8.youtube_category
),
category_name_added_to_old as (
    SELECT * EXCLUDE(category_id, youtube_category_id) FROM video_info_aws
    LEFT JOIN youtube_category
        ON youtube_category.youtube_category_id = video_info_aws.category_id 
),
category_name_added_to_new as (
    SELECT * EXCLUDE(category_id, youtube_category_id) FROM new_video_info_aws
    LEFT JOIN youtube_category
        ON youtube_category.youtube_category_id = new_video_info_aws.category_id
),
final AS (
    --SELECT * FROM {{ ref('video_info') }}
    SELECT * FROM category_name_added_to_old
    UNION
    SELECT * FROM category_name_added_to_new
)

SELECT * FROM final





/*
SELECT dt.value:categoryId_for_client_HyPeHoUsE::number
FROM {{ source('new_raw_data', 'new_video_info_aws') }}
    , LATERAL FLATTEN(INPUT => col1:data) as dt
*/