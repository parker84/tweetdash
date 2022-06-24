

select *
from app.user_scrape_status
where true
    and user_id = '{user_id}'
    and scrape_status = 'success'