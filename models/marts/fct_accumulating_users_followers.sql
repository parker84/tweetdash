

select 
    ifa.author_user_id,
    ifa.follower_user_id,
    --activation
    ifa.first_interaction_at as first_active_at,
    --eligibility
    case 
        when date_part('day', current_date - ifa.first_interaction_at) > 14 
        -- 14 bc you'll have 1 active week, and then we verify they have a full 2nd week to engage
        then true else false 
    end as eligible_1w_retention,
    case 
        when date_part('day', current_date - ifa.first_interaction_at) > 35
        -- 35 to ensure the follower has the full possible timespan to interact
        then true else false 
    end as eligible_4w_retention,
    --retention
    sum(
        case when trans.transition_number <= 2
        then trans.active_follower_transitions end
    ) as count_1w_retention, -- 1 or 0
    sum(
        case when trans.transition_number <= 5
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
from {{ ref('int_follower_attribution') }} as ifa
left join {{ ref('int_follower_transitions_with_transition_number') }} as trans using (author_user_id, follower_user_id)
group by 1,2,3,4,5