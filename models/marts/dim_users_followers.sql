


select 
    suf.author_user_id,
    suf.follower_user_id,
    --attribution
    ifa.first_interaction_at,
    ifa.first_interaction_tweet_id,
    dt.in_reply_to_user_id as acquired_from_reply_to_user_id,
    du.user_name as acquired_from_reply_to_user_name,
    --eligibility
    case 
        when date_part('day', current_date - ifa.first_interaction_at) > 14 
        -- 14 bc you'll have 1 active week, and then we verify they have a full 2nd week to engage
        then 1 else 0
    end as count_eligible_1w_retention,
    case 
        when date_part('day', current_date - ifa.first_interaction_at) > 35
        -- 35 to ensure the follower has the full possible timespan to interact
        then 1 else 0
    end as count_eligible_4w_retention
from {{ ref('stg_twitter__users_followers') }} as suf
left join {{ ref('int_follower_attribution') }} as ifa using (author_user_id, follower_user_id)
join {{ ref('dim_tweets') }} as dt on 
    ifa.first_interaction_tweet_id = dt.tweet_id
join {{ ref('dim_users') }} as du on 
    dt.in_reply_to_user_id = du.user_id