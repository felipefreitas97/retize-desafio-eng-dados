with instagram as (

    select
        m.id,
        'instagram' as platform,
        m.username,
        m.timestamp,

        trim(to_char(m.timestamp, 'Day')) as day_of_week,
        lower(m.media_product_type || '-' || m.media_type) as media_type,


        -- métricas base
        coalesce(i.likes, m.like_count) as likes,
        coalesce(i.comments, m.comments_count) as comments,
        coalesce(i.shares, 0) as shares,
        coalesce(i.saved, 0) as saves,  
        coalesce(i.reach, 0) as reach

    from "retize_dw"."social_media_staging"."stg_instagram_media" m
    left join "retize_dw"."social_media_staging"."stg_instagram_media_insights" i
        on m.id = i.id

),

tiktok_base as (

    select
        id,
        username,
        create_time as timestamp,
        likes,
        comments,
        shares,
        favorites,
        reach
    from "retize_dw"."social_media_staging"."stg_tiktok_posts"

),

tiktok as (

    select
        id,
        'tiktok' as platform,
        username,
        timestamp,

        trim(to_char(timestamp, 'Day')) as day_of_week,
        'tiktok-video' as media_type,

        -- métricas base
        coalesce(likes, 0) as likes,
        coalesce(comments, 0) as comments,
        coalesce(shares, 0) as shares,
        coalesce(favorites, 0) as saves,
        coalesce(reach, 0) as reach

    from tiktok_base

),

unioned as (

    select * from instagram
    union all
    select * from tiktok

),

final as (

    select
        id,
        platform,
        username,
        timestamp,
        day_of_week,
        media_type,
        likes,
        comments,
        shares,
        saves,
        reach,

        -- fórmula de engajamento
        case 
            when reach > 0 then
                (
                    (likes * 1) +
                    (comments * 3) +
                    (shares * 4) +
                    (saves * 3)
                )::float / reach
            else 0
        end as engagement

    from unioned

)

select * from final