select 
    distinct
    author_user_id as author_user_id,
    follower_user_id as follower_user_id,
    date_trunc(tweet_created_at, 'MONTH') as first_day_of_month,
from {{ ref('fct_periodic_likes_per_follower_per_month')}} as li
union distinct 
select 
    distinct
    author_user_id as author_user_id,
    follower_user_id as follower_user_id,
    date_trunc(tweet_created_at, 'MONTH') as first_day_of_month,
from {{ ref('fct_periodic_quotes_per_follower_per_month')}} as li
union distinct
select 
    distinct
    author_user_id as author_user_id,
    follower_user_id as follower_user_id,
    date_trunc(tweet_created_at, 'MONTH') as first_day_of_month,
from {{ ref('fct_periodic_replies_per_follower_per_month')}} as li
union distinct
select 
    distinct
    author_user_id as author_user_id,
    follower_user_id as follower_user_id,
    date_trunc(tweet_created_at, 'MONTH') as first_day_of_month,
from {{ ref('fct_periodic_retweets_per_follower_per_month')}} as li