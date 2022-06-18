
select 
    distinct
    cast(id as varchar) as user_id,
    cast(name as varchar) as user_name_readable,
    cast(username as varchar) as user_name,
    cast(description as varchar) as description,
    cast(public_metrics as varchar) as public_metrics,
    cast(url as varchar) as url,
    cast(location as varchar) as location,
from {{ source('twitter', 'user_meta_data') }}