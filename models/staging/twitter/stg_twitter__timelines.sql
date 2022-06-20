
select 
    id as tweet_id,
    lang as language,
    text as tweet_text,
    author_id,
    cast(created_at as timestamp) as tweet_created_at,
    in_reply_to_user_id,
    public_metrics
from {{ source('twitter', 'user_timelines') }}