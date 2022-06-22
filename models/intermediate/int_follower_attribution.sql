


with first_interaction_per_follower as (
    select 
        author_user_id,
        follower_user_id,
        min(tweet_created_at) as first_interaction_at
    from {{ ref('fct_transaction_interactions_tweets_followers') }}
    where count_interactions > 0
    group by 1,2
)

select 
    distinct
    fip.author_user_id, 
    fip.follower_user_id, 
    fip.first_interaction_at, 
    fti.tweet_id as first_interaction_tweet_id
from first_interaction_per_follower as fip
join {{ ref('fct_transaction_interactions_tweets_followers') }} as fti on
    fip.author_user_id = fti.author_user_id and 
    fip.follower_user_id = fti.follower_user_id and 
    fip.first_interaction_at = fti.tweet_created_at
join {{ ref('stg_twitter__users_followers') }} as uf on -- filter to actual followers
    fip.author_user_id = uf.author_user_id and 
    fip.follower_user_id = uf.follower_user_id