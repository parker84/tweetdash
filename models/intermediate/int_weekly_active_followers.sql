

with weekly_rollup as (
    select 
        author_user_id,
        follower_user_id,
        date_trunc('week', tweet_created_at) as week_begin_date,
        sum(count_interactions) as count_interactions,
        sum(count_likes) as count_likes,
        sum(count_replies) as count_replies,
        sum(count_retweets) as count_retweets,
        sum(count_quotes) as count_quotes
    from {{ ref('fct_transaction_interactions_tweets_followers') }}
    group by 1,2,3
)

select *,  
    case 
    when count_interactions > 0 then true else false
    end as is_active
from weekly_rollup