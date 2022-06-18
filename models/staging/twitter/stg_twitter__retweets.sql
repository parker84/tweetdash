
select 
    distinct
    cast(author_id as varchar) as author_user_id,
    cast(id as varchar) as follower_user_id,
    cast(tweet_id as varchar) as tweet_id,
    cast(tweet_created_at as timestamp) as tweet_created_at,
    cast(name as varchar) as user_name_readable,
    
    1 as count_retweets
from {{ source('twitter', 'tweets_retweeted') }}