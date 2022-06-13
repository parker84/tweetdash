
select 
    *
from {{ source('twitter', 'user_timelines') }}