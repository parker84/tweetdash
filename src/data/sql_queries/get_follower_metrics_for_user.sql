

select 
    duf.follower_user_name_readable,
    faf.first_active_at,
    faf.count_is_active as is_active,
    faf.count_likes,
    faf.count_replies,
    faf.count_retweets,
    faf.count_quotes,
    duf.acquired_from_reply_to_user_name,
    duf.count_followers_followers,
    duf.follower_description,
    faf.first_churn_at,
    faf.last_churn_at,
    faf.first_reactivated_at,
    faf.last_reactivated_at
from {schema}.dim_users_followers as duf
left join {schema}.fct_accumulating_users_followers as faf using (author_user_id, follower_user_id)
where author_user_id = '{user_id}'