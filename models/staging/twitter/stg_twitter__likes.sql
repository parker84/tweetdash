
select 
    distinct
    cast(author_id as varchar) as author_user_id,
    cast(id as varchar) as follower_user_id,
    cast(tweet_id as varchar) as tweet_id,
    cast(tweet_created_at as timestamp) as tweet_created_at,
    1 as count_likes
from {{ source('twitter', 'tweets_liked') }}