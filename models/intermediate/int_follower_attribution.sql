


with first_interaction_per_follower as (
    select 
        author_user_id,
        follower_user_id,
        min(tweet_created_at) as first_interaction_at
    from {{ ref('fct_transaction_interactions_per_tweet_and_follower') }}
    where count_interactions > 0
    group by 1,2
)

select 
    fip.author_user_id, 
    fip.follower_user_id, 
    fip.first_interaction_at, 
    fti.tweet_id as first_interaction_tweet_id
from first_interaction_per_follower as fip
join {{ ref('fct_transaction_interactions_per_tweet_and_follower') }} as fti on
    fip.author_user_id = fti.author_user_id and 
    fip.follower_user_id = fti.follower_user_id and 
    fip.first_interaction_at = fti.tweet_created_at 
