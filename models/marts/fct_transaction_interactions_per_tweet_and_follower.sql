


select 
    distinct
    ac.author_user_id,
    ac.follower_user_id,
    ac.tweet_id,
    ac.tweet_created_at,
    li.count_likes,
    qu.count_quotes,
    re.count_replies,
    ret.count_retweets,
    case 
        when 
            li.count_likes > 0 or 
            qu.count_quotes > 0 or
            re.count_replies > 0 or
            ret.count_retweets > 0
    then true else false end as is_interaction
from {{ ref('int_author_follower_combos_per_tweet') }} as ac
left join {{ ref('stg_twitter__likes') }} as li on
    ac.follower_user_id = li.follower_user_id and 
    ac.tweet_id = li.tweet_id
left join {{ ref('stg_twitter__quotes') }} as qu on
    ac.follower_user_id = qu.follower_user_id and 
    ac.tweet_id = qu.tweet_id
left join {{ ref('stg_twitter__replies') }} as re on
    ac.follower_user_id = re.follower_user_id and 
    ac.tweet_id = re.tweet_id
left join {{ ref('stg_twitter__retweets') }} as ret on
    ac.follower_user_id = ret.follower_user_id and 
    ac.tweet_id = ret.tweet_id
where 
    li.count_likes > 0 or 
    qu.count_quotes > 0 or
    re.count_replies > 0 or
    ret.count_retweets > 0

    