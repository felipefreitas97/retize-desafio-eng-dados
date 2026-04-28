#4. Quais são os 3 conteúdos com maior taxa de engajamento por conta no período, considerando Instagram e TikTok?

with ranked as (
    select
        *,
        row_number() over (
            partition by username
            order by engagement desc nulls last
        ) as rn
    from social_media_analytics.mart_posts_enriched
)

select *
from ranked
where rn <= 3;