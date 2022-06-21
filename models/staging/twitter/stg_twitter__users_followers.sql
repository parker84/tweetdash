
select 
    distinct
    cast(author_user_id as varchar) as author_user_id,
    cast(id as varchar) as follower_user_id,
    cast(name as varchar) as follower_user_name_readable,
    cast(username as varchar) as follower_user_name,
    cast(description as varchar) as follower_description,
    cast(public_metrics as varchar) as follower_public_metrics,
    cast(url as varchar) as follower_url,
    cast(location as varchar) as follower_location
from {{ source('twitter', 'users_followers') }}