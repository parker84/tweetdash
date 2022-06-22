
with tweets_retweeted_with_row_number as (
    select *,
        ROW_NUMBER() OVER (
            PARTITION BY tweet_id, id
            ORDER BY row_created_at DESC
        ) as row_number_per_tweet_and_follower_id_desc
    from {{ source('twitter', 'tweets_retweeted') }}
)

select 
    distinct
    cast(author_id as varchar) as author_user_id,
    cast(id as varchar) as follower_user_id,
    cast(tweet_id as varchar) as tweet_id,
    cast(tweet_created_at as timestamp) as tweet_created_at,
    1 as count_retweets
from tweets_retweeted_with_row_number
where row_number_per_tweet_and_follower_id_desc = 1