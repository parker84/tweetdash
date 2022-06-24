

with users_followers_with_row_number as (
    select *,
        ROW_NUMBER() OVER (
            PARTITION BY author_user_id, id
            ORDER BY row_created_at DESC
        ) as row_number_per_author_and_follower_desc
    from {{ source('twitter', 'users_followers') }}
    where public_metrics is not null
)

select 
    distinct
    cast(author_user_id as varchar) as author_user_id,
    cast(id as varchar) as follower_user_id,
    cast(name as varchar) as follower_user_name_readable,
    cast(username as varchar) as follower_user_name,
    cast(description as varchar) as follower_description,
    cast(url as varchar) as follower_url,
    cast(location as varchar) as follower_location,
    cast(row_created_at as timestamp) as follower_at,
    public_metrics::jsonb->'followers_count' as count_followers_followers,
    public_metrics::jsonb->'following_count' as count_followers_following,
    public_metrics::jsonb->'tweet_count' as count_followers_tweets,
    public_metrics::jsonb->'listed_count' as count_followers_listed
from users_followers_with_row_number
where row_number_per_author_and_follower_desc = 1