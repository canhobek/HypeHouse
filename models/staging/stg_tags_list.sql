WITH Tags AS (
    SELECT
        Video_ID
      , Trending_Date
      , index
      , Tags
    FROM {{ ref('video_info') }}
)
SELECT
      Video_ID
    , Trending_Date
    , Tags.index
    , Input.index     AS Tag_Index
    , Input.value     AS Tag_Value
FROM Tags, LATERAL split_to_table(Tags.Tags, ',') AS Input