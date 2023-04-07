with video_info_aws as (
    --SELECT * FROM raw_db.public.video_info_aws
    SELECT col1:categoryId_for_client_HyPeHoUsE::number                                     AS category_id
    , col1:channelId_for_client_HyPeHoUsE::varchar                                      AS channel_id
    , col1:"channelTitle                                      _for_client_HyPeHoUsE"::varchar               AS channel_title
    , col1:comments_disabled_for_client_HyPeHoUsE::boolean                              AS comments_disabled
    , col1:description_for_client_HyPeHoUsE::varchar                                    AS description
    , col1:dislikes_for_client_HyPeHoUsE::numeric                                       AS dislikes
    , col1:likes_for_client_HyPeHoUsE::varchar                                          AS likes
    , col1:publishedAt_for_client_HyPeHoUsE::datetime                                   AS published_at
    , col1:ratings_disabled_for_client_HyPeHoUsE::boolean                               AS ratings_disabled
    , col1:tags_for_client_HyPeHoUsE::varchar                                           AS tags
    , col1:thumbnail_link_for_client_HyPeHoUsE::varchar                                 AS thumbnail_link
    , col1:title_for_client_HyPeHoUsE::varchar                                          AS title
    , TO_DATE(col1:trending_date_for_client_HyPeHoUsE::varchar, 'YY.MM.DD')             AS trending_date
    , col1:video_id_for_client_HyPeHoUsE::varchar                                       AS video_id
    --FROM {{ ref('stg_video_info') }}
    FROM raw_db.public.video_info_aws
),
total_video_info as (
    SELECT * FROM video_info
),
final as (
    SELECt * FROM total_video_info
    UNION
    SELECT * FROM video_info_aws
)

SELECT * FROM final



/*
SELECT col1:categoryId_for_client_HyPeHoUsE::number                                     AS category_id
    , col1:channelId_for_client_HyPeHoUsE::varchar                                      AS channel_id
    , col1:"channelTitle                                      _for_client_HyPeHoUsE"::varchar               AS channel_title
    , col1:comments_disabled_for_client_HyPeHoUsE::boolean                              AS comments_disabled
    , col1:description_for_client_HyPeHoUsE::varchar                                    AS description
    , col1:dislikes_for_client_HyPeHoUsE::numeric                                       AS dislikes
    , col1:likes_for_client_HyPeHoUsE::varchar                                          AS likes
    , col1:publishedAt_for_client_HyPeHoUsE::datetime                                   AS published_at
    , col1:ratings_disabled_for_client_HyPeHoUsE::boolean                               AS ratings_disabled
    , col1:tags_for_client_HyPeHoUsE::varchar                                           AS tags
    , col1:thumbnail_link_for_client_HyPeHoUsE::varchar                                 AS thumbnail_link
    , col1:title_for_client_HyPeHoUsE::varchar                                          AS title
    , TO_DATE(col1:trending_date_for_client_HyPeHoUsE::varchar, 'YY.MM.DD')             AS trending_date
    , col1:video_id_for_client_HyPeHoUsE::varchar                                       AS video_id
FROM video_info_aws
*/