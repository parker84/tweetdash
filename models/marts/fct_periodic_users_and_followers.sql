


select 
    fmc.author_user_id,
    fmc.follower_user_id,
    fmc.first_day_of_month,
    count(distinct fpl.tweet_id) as count_likes,
    count(distinct fpq.tweet_id) as count_quotes,
    count(distinct fpr.tweet_id) as count_replies,
    count(distinct fprt.tweet_id) as count_retweets,
from {{ ref('fct_factless_author_follower_month_combos') }} as fmc
left join {{ ref('fct_periodic_likes_per_follower_per_month') }} as fpl on
    fmc.author_user_id = fpl.author_user_id and 
    fmc.follower_user_id = fpl.follower_user_id and 
    fmc.first_day_of_month = fpl.first_day_of_month
left join {{ ref('fct_periodic_quotes_per_follower_per_month') }} as fpq on
    fmc.author_user_id = fpq.author_user_id and 
    fmc.follower_user_id = fpq.follower_user_id and 
    fmc.first_day_of_month = fpq.first_day_of_month
left join {{ ref('fct_periodic_replies_per_follower_per_month') }} as fpr on
    fmc.author_user_id = fpr.author_user_id and 
    fmc.follower_user_id = fpr.follower_user_id and 
    fmc.first_day_of_month = fpr.first_day_of_month
left join {{ ref('fct_periodic_retweets_per_follower_per_month') }} as fprt on
    fmc.author_user_id = fprt.author_user_id and 
    fmc.follower_user_id = fprt.follower_user_id and 
    fmc.first_day_of_month = fprt.first_day_of_month



    