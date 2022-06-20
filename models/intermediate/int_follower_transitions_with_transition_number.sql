

select 
    *, ROW_NUMBER() OVER (
        PARTITION BY author_user_id, follower_user_id
        ORDER BY week_begin_date
    ) as transition_number
from {{ ref('fct_transaction_weekly_active_followers_transitions') }}

