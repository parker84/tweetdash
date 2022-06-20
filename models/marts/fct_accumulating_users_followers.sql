


select 
    author_user_id,
    follower_user_id,
    --attribution
    min(first_interaction_at) as first_active_at
from {{ ref('int_follower_attribution') }} as ifa
group by 1,2