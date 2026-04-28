with source as (

    select *
    from "retize_dw"."social_media_raw"."raw_instagram_media"

),

final as (

    select
        id,
        username,
        content_source,

        cast(timestamp as timestamp) - interval '3 hours' as timestamp,

        upper(media_type) as media_type,

        cast(like_count as int) as like_count,
        cast(comments_count as int) as comments_count,

        is_comment_enabled,
        media_product_type

    from source

)

select * from final