with instagram as (

    select
        c.comment_id,
        c.post_id,
        'instagram' as platform,
        m.username,
        lower(m.media_product_type || '-' || m.media_type) as media_type,
    

        c.comment_timestamp,
        c.day_of_week,

        c.predicted_sentiment,
        c.sentiment_value,
        c.confidence_sentiment,
        c.value_sentiment

    from "retize_dw"."social_media_staging"."stg_instagram_comments" c
    left join "retize_dw"."social_media_staging"."stg_instagram_media" m
        on c.post_id = m.id

),

tiktok_base as (

    select
        id,
        username,
        create_time as timestamp
    from "retize_dw"."social_media_staging"."stg_tiktok_posts"

),

tiktok as (

    select
        c.comment_id,
        c.post_id,
        'tiktok' as platform,
        p.username,

        'tiktok-video' as media_type,

        c.comment_timestamp,
        c.day_of_week,

        c.predicted_sentiment,
        c.sentiment_value,
        c.confidence_sentiment,
        c.value_sentiment

    from "retize_dw"."social_media_staging"."stg_tiktok_comments" c
    left join tiktok_base p
        on c.post_id = p.id

),

unioned as (

    select * from instagram
    union all
    select * from tiktok

)

select * from unioned