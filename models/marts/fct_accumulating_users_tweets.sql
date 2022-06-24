


select 
    --grain
    dt.author_user_id,
    dt.tweet_id,
    --attributions
    sum(fau.count_is_active) as count_new_active_followers,
    sum(fau.count_1w_retention) / sum(fau.count_eligible_1w_retention) as active_follower_retention_1w,
    sum(fau.count_4w_retention) / sum(fau.count_eligible_4w_retention) as active_follower_retention_4w
from {{ ref('dim_tweets') }} as dt
left join {{ ref('dim_users_followers') }} as duf on
    dt.tweet_id = duf.first_interaction_tweet_id
left join {{ ref('fct_accumulating_users_followers') }} as fau on
    duf.author_user_id = fau.author_user_id and 
    duf.follower_user_id = fau.follower_user_id
group by 1,2