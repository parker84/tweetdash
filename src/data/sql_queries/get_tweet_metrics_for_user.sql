

select td.*, fat.count_new_active_followers, fat.active_follower_retention_1w, fat.active_follower_retention_4w
from {schema}.dim_tweets as td 
left join {schema}.fct_accumulating_users_tweets as fat using (tweet_id)
where td.author_user_id = '{user_id}'