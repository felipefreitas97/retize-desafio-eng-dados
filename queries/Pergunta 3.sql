#3. Quais são os 10 conteúdos com maior taxa de engajamento no período, considerando Instagram e TikTok?


select
    id as post_id,
    username,
    platform,
    media_type,
    engagement,
    reach,
    likes,
    comments,
    shares,
    saves
from social_media_analytics.mart_posts_enriched
order by engagement desc nulls last
limit 10;