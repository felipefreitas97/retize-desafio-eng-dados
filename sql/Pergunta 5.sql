#5. Para cada conta, qual formato de conteúdo apresenta o melhor desempenho médio de engajamento?


with agg as (
    select
        username,
        media_type,
        avg(engagement) as avg_engagement
    from social_media_analytics.mart_posts_enriched
    where media_type is not null
    group by username, media_type
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
