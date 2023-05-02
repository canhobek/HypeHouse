with likes_dislikes_count AS (
    SELECT DISTINCT dt.value:index::varchar                              AS index
    , dt.value:video_id_for_client_HyPeHoUsE::varchar                                AS video_id
    , dt.value:likes_for_client_HyPeHoUsE::number               AS likes
    , dt.value:dislikes_for_client_HyPeHoUsE::number            AS dislikes
    , trend_date
    FROM {{ source('new_raw_data', 'new_likes_count_aws') }}
    , LATERAL FLATTEN(INPUT => col1:data) as dt
),
final as (
    SELECT * FROM likes_dislikes_count
)

SELECT * FROM final
