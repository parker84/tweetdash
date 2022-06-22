

with users_followers_with_row_number as (
    select *,
        ROW_NUMBER() OVER (
            PARTITION BY author_user_id, id
            ORDER BY row_created_at DESC
        ) as row_number_per_author_and_follower_desc
    from {{ source('twitter', 'users_followers') }}
)

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
from users_followers_with_row_number
where row_number_per_author_and_follower_desc = 1