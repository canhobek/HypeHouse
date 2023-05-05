WITH Tags AS (
    SELECT
        Video_ID
      , Trending_Date
      --, index
      , Tags
    FROM {{ ref('video_info') }}
)
SELECT
      Video_ID
    , Trending_Date
    --, Tags.index
    , split_tags.index     AS Tag_Index
    , split_tags.value     AS Tag_Value
FROM Tags, LATERAL split_to_table(Tags.Tags, ',') AS split_tags