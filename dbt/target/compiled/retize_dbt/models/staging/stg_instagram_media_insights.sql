with source as (

    select *
    from "retize_dw"."social_media_raw"."raw_instagram_media_insights"

),

final as (

    select
        id,
        content_source,

        coalesce(likes , 0) as likes,
        coalesce(reach , 0) as reach,
        coalesce(saved , 0) as saved,
        coalesce(views , 0) as views,
        coalesce(shares , 0) as shares,
        coalesce(follows , 0) as follows,
        coalesce(comments , 0) as comments,
        coalesce(replies , 0) as replies,
        coalesce(profile_visits , 0) as profile_visits,
        coalesce(total_interactions , 0) as total_interactions

    from source

)

select * from final