--> materialized view because I want to make automatic record changes

SELECT video_id
    --, COUNT(video_id) as frequency
    , MIN(trending_date) AS first_trending_date
    , MAX(trending_date) AS last_trending_date
    , DATEDIFF(month, first_trending_date, last_trending_date) AS number_of_days_in_trend
FROM {{ ref('video_info') }}
GROUP BY video_id


