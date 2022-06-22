
with user_meta_data as (
    select *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY row_created_at DESC
        ) as update_number_desc
    from {{ source('twitter', 'user_meta_data') }}
)

select 
    cast(id as varchar) as user_id,
    cast(name as varchar) as user_name_readable,
    cast(username as varchar) as user_name,
    cast(description as varchar) as description,
    cast(public_metrics as varchar) as public_metrics,
    cast(url as varchar) as url,
    cast(location as varchar) as location
from user_meta_data
where update_number_desc = 1