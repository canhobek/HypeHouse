with new_comment_count_aws as (
    SELECT DISTINCT dt.value:index::number                                 AS index
    , dt.value:video_id_for_client_HyPeHoUsE::varchar             AS video_id
    , dt.value:comment_count_for_client_HyPeHoUsE::varchar        AS comment_count --> DONT FORGET TO CHANGE THIS TO A NUMBER
    , trend_date
    FROM {{ source('new_raw_data', 'new_comment_count_aws') }}
    , LATERAL FLATTEN(INPUT => col1:data) as dt
),
old_comment_count_aws as (
    SELECT null                                         AS index
        , col1:video_id::varchar                        AS video_id
        , col1:comment_count::varchar                   AS comment_count --> DONT FORGET TO CHANGE THIS TO NUMBER
        , trend_date                              
    --FROM {{ ref('stg_comment_count') }}
    --FROM raw_db.public.comment_count_aws
    FROM {{ source('public', 'comment_count_aws') }}
    WHERE video_id NOT LIKE 'video_id'
),
final as (
    SELECT * FROM old_comment_count_aws
    UNION
    SELECT * FROM new_comment_count_aws
)

SELECT * FROM final
