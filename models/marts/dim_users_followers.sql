


select 
    ifa.author_user_id,
    ifa.follower_user_id,
    --attribution
    ifa.first_interaction_at,
    ifa.first_interaction_tweet_id,
    dt.in_reply_to_user_id as acquired_from_reply_to_user_id,
    du.user_name as acquired_from_reply_to_user_name
from {{ ref('int_follower_attribution') }} as ifa
join {{ ref('dim_tweets') }} as dt on 
    ifa.first_interaction_tweet_id = dt.tweet_id
join {{ ref('dim_users') }} as du on 
    dt.in_reply_to_user_id = du.user_id