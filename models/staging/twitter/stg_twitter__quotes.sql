
select 
    distinct
    cast(author_id as varchar) as author_user_id,
    cast(id as varchar) as follower_user_id,
    cast(tweet_id as varchar) as tweet_id,
    cast(tweet_created_at as timestamp) as tweet_created_at,
    cast(name as varchar) as user_name_readable,
    cast(username as varchar) as user_name
from {{ source('twitter', 'tweets_replied_to') }}