SELECT vid_inf.video_id
    , vid_inf.category_name
    , vid_inf.channel_id
    , vid_inf.channel_title
    , vid_inf.likes
    , vid_inf.dislikes
    , vid_inf.published_at
    , vid_inf.trending_date
    , vid_inf.tags
    , vid_inf.title
    , com_cou.comment_count
    , vie_cou.view_count
    , vid_tre_rec.first_trending_date
    , vid_tre_rec.last_trending_date
    , vid_tre_rec.number_of_days_in_trend
FROM {{ ref('video_info') }} AS vid_inf
INNER JOIN {{ ref('comment_count') }} AS com_cou
    ON com_cou.video_id = vid_inf.video_id AND com_cou.trend_date = vid_inf.trending_date
INNER JOIN {{ ref('view_count') }} AS vie_cou
    ON vie_cou.video_id = vid_inf.video_id AND vie_cou.trend_date = vid_inf.trending_date
INNER JOIN {{ ref('video_trend_record') }} AS vid_tre_rec
    ON vid_tre_rec.video_id = vid_inf.video_id
