with new_view_count_aws as (
    SELECT DISTINCT dt.value:index::number                                      AS index
        , dt.value:video_id_for_client_HyPeHoUsE::varchar              AS video_id
        , dt.value:view_count_for_client_HyPeHoUsE::number             AS view_count
        , trend_date
    FROM {{ source('new_raw_data', 'new_view_count_aws') }}
    , LATERAL FLATTEN(INPUT => col1:data) as dt
),
old_view_count_aws as (
    SELECT null                           AS index
        , col1:video_id::varchar          AS video_id
        , col1:view_count::number         AS view_count
        , trend_date
    --FROM {{ ref('stg_view_count') }}
    --FROM raw_db.public.view_count_aws
    FROM {{ source('public', 'view_count_aws') }}
    WHERE video_id NOT LIKE 'video_id'
),
final as (
    SELECT * FROM old_view_count_aws
    UNION
    SELECT * FROM new_view_count_aws
)

SELECT * FROM final