with new_day_comment_count as (
    SELECT col1:video_id::varchar                       AS video_id
        , col1:comment_count::numeric                   AS comment_count
        , trend_date                              
    --FROM {{ ref('stg_comment_count') }}
    --FROM raw_db.public.comment_count_aws
    FROM {{ source('public', 'comment_count_aws') }}
    WHERE video_id NOT LIKE 'video_id'
),
/*
total_comment_count as (
    SELECT * FROM ANALYTICS.DBT_HH8.COMMENT_COUNT
),*/
/*
final as (
    SELECT * FROM total_comment_count
    UNION
    SELECT * FROM new_day_comment_count
)
*/
final as (
    SELECT * FROM new_day_comment_count
)

SELECT * FROM final


/*
SELECT col1:video_id::varchar                       AS video_id
    , col1:comment_count::numeric                   AS comment_count
FROM comment_count_aws


SELECT * FROM comment_count;*/