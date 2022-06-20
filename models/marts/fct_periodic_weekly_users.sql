



select 
    author_user_id,
    week_begin_date,
    --active follower metrics
    sum(count_active_followers) as count_active_followers,
    sum(count_eligible_active_followers_retained_1w) as count_eligible_active_followers_retained_1w,
    sum(count_eligible_active_followers_retained_4w) as count_eligible_active_followers_retained_4w,
    sum(count_active_followers_retained_1w) as count_active_followers_retained_1w,
    sum(count_active_followers_retained_4w) as count_active_followers_retained_4w,
    --engagement
    sum(count_followers_that_interacted) as count_followers_that_interacted,
    sum(count_followers_that_liked) as count_followers_that_liked,
    sum(count_followers_that_replied) as count_followers_that_replied,
    sum(count_followers_that_retweeted) as count_followers_that_retweeted,
    sum(count_followers_that_quoted) as count_followers_that_quoted
from {{ ref('fct_periodic_weekly_users_followers') }}
group by 1,2