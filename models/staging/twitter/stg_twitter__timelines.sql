with user_timelines_with_row_number as (
    select *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY row_created_at DESC
        ) as row_number_per_tweet_id_desc
    from {{ source('twitter', 'user_timelines') }}
)
select
    id as tweet_id,
    lang as language,
    text as tweet_text,
    author_id as author_user_id,
    cast(created_at as timestamp) as tweet_created_at,
    in_reply_to_user_id,
    public_metrics::json->'retweet_count' as count_retweets,
    public_metrics::json->'like_count' as count_likes,
    public_metrics::json->'quote_count' as count_quotes,
    public_metrics::json->'reply_count' as count_replies
from user_timelines_with_row_number
where row_number_per_tweet_id_desc = 1