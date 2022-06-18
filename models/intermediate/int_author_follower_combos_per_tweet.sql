select 
    distinct
    author_user_id,
    follower_user_id,
    tweet_created_at,
    tweet_id
from {{ ref('stg_twitter__likes') }}
union distinct 
select 
    distinct
    author_user_id,
    follower_user_id,
    tweet_created_at,
    tweet_id
from {{ ref('stg_twitter__quotes') }}
union distinct
select 
    distinct
    author_user_id,
    follower_user_id,
    tweet_created_at,
    tweet_id
from {{ ref('stg_twitter__replies') }}
union distinct
select 
    distinct
    author_user_id,
    follower_user_id,
    tweet_created_at,
    tweet_id
from {{ ref('stg_twitter__retweets') }}