
  create view "retize_dw"."social_media_staging"."stg_tiktok_posts__dbt_tmp"
    
    
  as (
    with source as (

    select *
    from "retize_dw"."social_media_raw"."raw_tiktok_posts"

),

final as (

    select
        item_id as id,
        business_username as username,

        -- timestamp padronizado
        cast(create_time as timestamp) - interval '3 hours' as create_time,

        -- dia da semana pt-br
        case extract(dow from cast(create_time as timestamp) - interval '3 hours')
            when 0 then 'domingo'
            when 1 then 'segunda'
            when 2 then 'terca'
            when 3 then 'quarta'
            when 4 then 'quinta'
            when 5 then 'sexta'
            when 6 then 'sabado'
        end as create_day_of_week,

        coalesce(likes ,0) as likes,
        coalesce(comments ,0) as comments,
        coalesce(shares ,0) as shares,
        coalesce(video_views ,0) as video_views,
        coalesce(reach ,0) as reach,
        coalesce(favorites ,0) as favorites,
        coalesce(profile_views ,0) as profile_views,
        coalesce(new_followers ,0) as new_followers,
        coalesce(total_time_watched ,0) as total_time_watched,
        coalesce(average_time_watched , 0.0) as average_time_watched,
        coalesce(full_video_watched_rate , 0.0) as full_video_watched_rate,
        coalesce(video_duration , 0.0) as video_duration,
        coalesce(app_download_clicks ,0) as app_download_clicks,
        coalesce(lead_submissions ,0) as lead_submissions,
        coalesce(phone_number_clicks ,0) as phone_number_clicks,
        coalesce(website_clicks ,0) as website_clicks

    from source

)

select * from final
  );