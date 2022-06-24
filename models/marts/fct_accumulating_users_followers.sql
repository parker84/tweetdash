

select 
    duf.author_user_id,
    duf.follower_user_id,
    --eligible
    duf.count_eligible_1w_retention as count_eligible_1w_retention,
    duf.count_eligible_4w_retention as count_eligible_4w_retention,
    --activation
    duf.first_interaction_at as first_active_at,
    sum(trans.active_follower_transitions) as count_is_active, -- 1 or 0
    --engagement
    sum(act.count_interactions) as count_interactions,
    sum(act.count_likes) as count_likes,
    sum(act.count_replies) as count_replies,
    sum(act.count_retweets) as count_retweets,
    sum(act.count_quotes) as count_quotes,
    --retention
    sum(
        case 
        when trans.transition_number <= 2 and duf.count_eligible_1w_retention=1
        then trans.active_follower_transitions end
    ) as count_1w_retention, -- 1 or 0
    sum(
        case 
        when trans.transition_number <= 5 and duf.count_eligible_4w_retention=1
        then trans.active_follower_transitions  end
    ) as count_4w_retention, -- 1 or 0
    --deactivation / churn
    min(
        case when trans.active_follower_transitions = -1
        then trans.week_begin_date end
    ) as first_churn_at,
    max(
        case when trans.active_follower_transitions = -1
        then trans.week_begin_date end
    ) as last_churn_at,
    --reactivation
    min(
        case when trans.transition_number > 1 and trans.active_follower_transitions = 1
        then trans.week_begin_date end
    ) as first_reactivated_at,
    max(
        case when trans.transition_number > 1 and trans.active_follower_transitions = 1
        then trans.week_begin_date end
    ) as last_reactivated_at
from {{ ref('dim_users_followers') }} as duf
left join {{ ref('int_weekly_active_followers_transitions') }} as trans using (author_user_id, follower_user_id)
left join {{ ref('int_weekly_active_followers') }} as act using (author_user_id, follower_user_id, week_begin_date)
where true
    and trans.week_begin_date < (select max(week_begin_date) from {{ ref('int_weekly_active_followers_transitions') }})
group by 1,2,3,4,5