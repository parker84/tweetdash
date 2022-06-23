

with weekly_active_stats as (
    select 
        duf.author_user_id,
        duf.follower_user_id,
        trans.week_begin_date,
        --active
        sum(
            case when act.is_active
            then 1 else 0 end
        ) as count_active_followers,
        --changes
        sum(trans.active_follower_transitions) as count_net_active_followers,
        sum(trans.new_active_followers) as count_new_active_followers,
        sum(trans.churned_active_followers) as count_churned_active_followers
    from {{ ref('dim_users_followers') }} as duf
    left join {{ ref('int_weekly_active_followers_transitions') }} as trans using (author_user_id, follower_user_id)
    left join {{ ref('int_weekly_active_followers') }} as act using (author_user_id, follower_user_id, week_begin_date)
    group by 1,2,3
)

, weekly_engagement as (
    select
        author_user_id,
        follower_user_id,
        week_begin_date,
        sum(
            case 
            when count_interactions > 0 then 1 else 0
            end 
        ) as count_followers_that_interacted,
        sum(
            case 
            when count_likes > 0 then 1 else 0
            end 
        ) as count_followers_that_liked,
        sum(
            case 
            when count_replies > 0 then 1 else 0
            end 
        ) as count_followers_that_replied,
        sum(
            case 
            when count_retweets > 0 then 1 else 0
            end 
        ) as count_followers_that_retweeted,
        sum(
            case 
            when count_quotes > 0 then 1 else 0
            end 
        ) as count_followers_that_quoted
    from {{ ref('int_weekly_active_followers') }}
    group by 1,2,3
)

select 
    was.author_user_id,
    was.follower_user_id,
    was.week_begin_date,
    --active follower metrics
    was.count_active_followers,
    was.count_net_active_followers,
    was.count_new_active_followers,
    was.count_churned_active_followers,
    --engagement
    wen.count_followers_that_interacted,
    wen.count_followers_that_liked,
    wen.count_followers_that_replied,
    wen.count_followers_that_retweeted,
    wen.count_followers_that_quoted
from weekly_active_stats as was
left join weekly_engagement as wen using (author_user_id, follower_user_id, week_begin_date)