with view_count_aws as (
    --SELECT * FROM raw_db.public.view_count_aws
    SELECT col1:video_id::varchar         AS video_id
        , col1:view_count::number         AS view_count
    --FROM {{ ref('stg_view_count') }}
    FROM raw_db.public.view_count_aws
),
total_view_count as (
    SELECT * FROM ANALYTICS.DBT_HH8.view_count
),
final as (
    SELECT * FROM total_view_count
    UNION
    SELECT * FROM view_count_aws
)

SELECT * FROM final


/*
SELECT col1:video_id::varchar         AS video_id
    , col1:view_count::number         AS view_count
FROM view_count_aws
*/