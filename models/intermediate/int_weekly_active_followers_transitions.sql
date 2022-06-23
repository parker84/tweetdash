

with weekly_active_followers as (
    select 
        *,
        week_begin_date + INTERVAL '7 days' as week_begin_date_add_7d,
        week_begin_date - INTERVAL '7 days' as week_begin_date_minus_7d
    from {{ ref('int_weekly_active_followers') }}
)

, author_follower_week_combos as (
    select distinct 
        author_user_id, follower_user_id, week_begin_date --the exact active weeks
    from weekly_active_followers
    union distinct 
    select distinct 
        author_user_id, follower_user_id, week_begin_date_add_7d as week_begin_date --the weeks after
    from weekly_active_followers
    union distinct 
    select distinct 
        author_user_id, follower_user_id, week_begin_date_minus_7d as week_begin_date --the weeks before
    from weekly_active_followers
)

, transitions as (
    select 
        --keys
        afw.author_user_id,
        afw.follower_user_id,
        afw.week_begin_date,
        --transitions
        case 
            when 
                waf.is_active and (waf_lag.is_active is null or waf_lag.is_active = false)
                then 1 -- activated
            when 
                waf_lag.is_active and (waf.is_active is null or waf.is_active = false)
                then -1 -- churned
            else 0 -- stayed the same
        end as active_follower_transitions, --sum this to get whether the follower is active at some point 
        --qa
        waf_lag.week_begin_date as qa_last_week_begin_date,
        waf_lead.week_begin_date as qa_next_week_begin_date,
        waf.is_active as qa_active_this_week,
        waf_lag.is_active as qa_active_last_week,
        waf_lead.is_active as qa_active_next_week
    from author_follower_week_combos as afw
    left join weekly_active_followers as waf on
        afw.author_user_id = waf.author_user_id and 
        afw.follower_user_id = waf.follower_user_id and 
        afw.week_begin_date = waf.week_begin_date
    left join weekly_active_followers as waf_lag on
        afw.author_user_id = waf_lag.author_user_id and 
        afw.follower_user_id = waf_lag.follower_user_id and 
        afw.week_begin_date = waf_lag.week_begin_date_add_7d
    left join weekly_active_followers as waf_lead on
        afw.author_user_id = waf_lead.author_user_id and 
        afw.follower_user_id = waf_lead.follower_user_id and 
        afw.week_begin_date = waf_lead.week_begin_date_minus_7d
    where true
        and afw.week_begin_date < (select max(week_begin_date) from weekly_active_followers)
        and afw.week_begin_date > (select min(week_begin_date) from weekly_active_followers)
)

select 
    *, ROW_NUMBER() OVER (
        PARTITION BY author_user_id, follower_user_id
        ORDER BY week_begin_date
    ) as transition_number,
    case 
        when active_follower_transitions > 0 
        then 1 else 0
    end as new_active_followers, 
    case 
        when active_follower_transitions < 0 
        then 1 else 0
    end as churned_active_followers
from transitions

