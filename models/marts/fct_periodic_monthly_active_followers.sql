
select 
    author_user_id,
    first_day_of_month,
    count(distinct follower_user_id) as count_active_followers
from fct_factless_author_follower_month_combos 