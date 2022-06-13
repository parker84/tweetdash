
select 
    distinct
    cast(id as varchar) as user_id,
    name as user_name_readable,
    username as user_name
from {{ source('twitter', 'tweets_liked') }}