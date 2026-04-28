#2. Para cada conta, qual plataforma apresentou a maior proporção de comentários negativos no período analisado?

# Cálculo da proporção de comentários negativos por plataforma e conta, e identificação da plataforma com maior proporção para cada conta.
# Esse cálculo pega o valor médio de sentimento (avg_sentiment) para cada conta e plataforma, e classifica 
#as plataformas para cada conta com base nesse valor. A plataforma com o menor avg_sentiment (mais negativa) recebe a classificação 1.

with base as (
    select
        username,
        platform,
        count(*) as total_comments,

        -- proporção negativa (mantém)
        count(*) filter (
            where predicted_sentiment = 'negativo'
        ) as negative_comments,

        avg(value_sentiment) as avg_sentiment

    from social_media_analytics.mart_comments_enriched
    group by username, platform
),

calc as (
    select
        *,
        negative_comments::float / total_comments as negative_ratio
    from base
),

ranked as (
    select *,
        row_number() over (
            partition by username
            order by avg_sentiment asc 
        ) as rn
    from calc
)

select *
from ranked
where rn = 1;
