

select 
    date_trunc('week', first_active_at) as first_active_week_begin_date,
    sum(count_1w_retention) / sum(count_eligible_1w_retention) as retention_rate_1w,
    sum(count_4w_retention) / sum(count_eligible_4w_retention) as retention_rate_4w
from {schema}.fct_accumulating_users_followers
where true 
    and author_user_id = '{user_id}'
    and first_active_at is not null
group by 1