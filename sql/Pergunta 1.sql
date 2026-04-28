#1. Para cada conta, qual é o melhor dia da semana para publicar, considerando o engajamento médio por conteúdo?

with agg as (
    select
        username,
        day_of_week,
        avg(engagement) as avg_engagement
    from social_media_analytics.mart_posts_enriched
    group by username, day_of_week
),

ranked as (
    select *,
        row_number() over (
            partition by username
            order by avg_engagement desc
        ) as rn
    from agg
)

select *
from ranked
where rn = 1;