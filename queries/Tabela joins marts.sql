with comments as (

    select
        comment_id,
        post_id,
        platform,
        username,
        media_type,
        comment_timestamp,
        day_of_week as comment_day_of_week,
        predicted_sentiment,
        value_sentiment

    from social_media_analytics.mart_comments_enriched

),

posts as (

    select
        id as post_id,
        platform,
        username as post_username,
        media_type as post_media_type,
        "timestamp" as post_timestamp,
        day_of_week as post_day_of_week,
        likes,
        comments as total_comments,
        shares,
        saves,
        reach,
        engagement

    from social_media_analytics.mart_posts_enriched

),

joined as (

    select
        c.comment_id,
        c.post_id,
        c.platform,

        p.post_username,

        coalesce(c.media_type, p.post_media_type) as media_type,

        c.comment_timestamp,
        c.comment_day_of_week,
        p.post_timestamp,
        p.post_day_of_week,

        c.predicted_sentiment,
        c.value_sentiment,

        p.likes,
        p.total_comments,
        p.shares,
        p.saves,
        p.reach,
        p.engagement

    from comments c
    left join posts p
        on c.post_id = p.post_id
        and c.platform = p.platform

)

select *
from joined;