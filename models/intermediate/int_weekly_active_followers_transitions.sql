

with weekly_active_followers as (
    select 
        *,
        week_begin_date + INTERVAL '7 days' as week_begin_date_add_7d
    from {{ ref('int_weekly_active_followers') }}
)

, transitions as (
    select 
        --keys
        coalesce(waf.author_user_id, waf_lagged.author_user_id) as author_user_id,
        coalesce(waf.follower_user_id, waf_lagged.follower_user_id) as follower_user_id,
        coalesce(waf.week_begin_date, waf_lagged.week_begin_date) as week_begin_date,
        --transitions
        case 
            when 
                waf.is_active and (waf_lagged.is_active is null or waf_lagged.is_active = false)
                then 1 -- activated
            when 
                waf_lagged.is_active and (waf.is_active is null or waf.is_active = false)
                then -1 -- churned
            else 0 -- stayed the same
        end as active_follower_transitions, --sum this to get whether the follower is active at some point 
        --qa
        waf_lagged.week_begin_date as qa_last_week_begin_date
    from weekly_active_followers as waf
    full outer join weekly_active_followers as waf_lagged on
        waf.author_user_id = waf_lagged.author_user_id and 
        waf.follower_user_id = waf_lagged.follower_user_id and 
        waf.week_begin_date = waf_lagged.week_begin_date_add_7d
)

select 
    *, ROW_NUMBER() OVER (
        PARTITION BY author_user_id, follower_user_id
        ORDER BY week_begin_date
    ) as transition_number
from transitions

