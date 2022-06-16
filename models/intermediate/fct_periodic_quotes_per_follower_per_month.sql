


select 
    author_user_id,
    follower_user_id,
    date_trunc(tweet_created_at, 'MONTH') first_day_of_month,
    count(distinct tweet_id) as count_quotes
from {{ ref('stg_twitter__quotes') }}
group by 1,2,3