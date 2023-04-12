with view_count_aws as (
    SELECT col1:video_id::varchar         AS video_id
        , col1:view_count::number         AS view_count
        , trend_date
    --FROM {{ ref('stg_view_count') }}
    --FROM raw_db.public.view_count_aws
    FROM {{ source('public', 'view_count_aws') }}
    WHERE video_id NOT LIKE 'video_id'
),
total_view_count as (
    SELECT * FROM ANALYTICS.DBT_HH8.view_count
),
/*
final as (
    SELECT * FROM total_view_count
    UNION
    SELECT * FROM view_count_aws
)
*/
final as (
    SELECT * FROM view_count_aws
)

SELECT * FROM final


/*
SELECT col1:video_id::varchar         AS video_id
    , col1:view_count::number         AS view_count
FROM view_count_aws
*/