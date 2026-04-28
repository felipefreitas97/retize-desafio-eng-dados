
  create view "retize_dw"."social_media_staging"."stg_tiktok_comments__dbt_tmp"
    
    
  as (
    with source as (

    select *
    from "retize_dw"."social_media_raw"."raw_tiktok_comments"

),

final as (

    select
        post_id,
        comment_id,

        cast(comment_timestamp as timestamp) - interval '3 hours' as comment_timestamp,

        case extract(dow from cast(comment_timestamp as timestamp) - interval '3 hours')
            when 0 then 'domingo'
            when 1 then 'segunda'
            when 2 then 'terca'
            when 3 then 'quarta'
            when 4 then 'quinta'
            when 5 then 'sexta'
            when 6 then 'sabado'
        end as day_of_week,

        lower(predicted_sentiment) as predicted_sentiment,

        case 
            when lower(predicted_sentiment) = 'positivo' then 1
            when lower(predicted_sentiment) = 'neutro' then 0
            when lower(predicted_sentiment) = 'negativo' then -1
        end as sentiment_value,

         cast(confidence_sentiment as float) as confidence_sentiment,

        (
            case 
                when lower(predicted_sentiment) = 'positivo' then 1
                when lower(predicted_sentiment) = 'neutro' then 0
                when lower(predicted_sentiment) = 'negativo' then -1
                else 0
            end
        ) * cast(confidence_sentiment as float) as value_sentiment

    from source

)

select * from final
  );