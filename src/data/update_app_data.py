import pandas as pd
from datetime import datetime
from settings import db_conn


def update_user_scrape_status_to_success(user_id):
    df = pd.DataFrame([], columns=['user_id', 'scrape_status', 'row_created_at'])
    df.append([user_id, 'success', datetime.now()])
    # TODO: add an email here that will send to the user with streamlit dash details
    df.to_sql(
        name='user_scrape_status',
        con=db_conn,
        if_exists='append',
        schema='app'
    )
    
def save_user_login_event(user_name, user_email):
    df = pd.DataFrame([], columns=['user_name', 'email', 'event', 'row_created_at'])
    df.append([user_name, user_email, 'login', datetime.now()])
    df.to_sql(
        name='user_emails',
        con=db_conn,
        if_exists='append',
        schema='app'
)