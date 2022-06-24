from sqlalchemy import create_engine
from decouple import config
import pandas as pd
import streamlit as st
import coloredlogs, logging
logger = logging.getLogger(__name__)
coloredlogs.install(level=config('LOG_LEVEL'))

ENGINE_PATH = f"postgresql://{config('DB_USER')}:{config('DB_PWD')}@{config('DB_HOST')}/{config('DB')}"
BEARER_TOKEN = config("BEARER_TOKEN")

engine = create_engine(ENGINE_PATH)
db_conn = engine.connect()



class UserData():

    def __init__(self, user_id) -> None:
        self.user_id = user_id
    
    def _run_query_and_return_df(self, query_filename: str):
        with open(f'./src/data/sql_queries/{query_filename}.sql', 'r') as f:
            query = f.read().format(user_id=self.user_id, schema=config('DB_SCHEMA'))
        logger.info(f'Query: {query_filename} {query}\n')
        df = pd.read_sql(query, db_conn)
        return df

    @st.cache
    def check_if_new_user(self):
        try:
            df = self._run_query_and_return_df('get_user_scrape_status')
            is_new_user = df.shape[0] == 0
        except Exception as err:
            logger.warn(f'Error trying to query dim_users: {err}')
            is_new_user = True
        return is_new_user

    @st.cache
    def get_weekly_metrics_for_user(self):
        df = self._run_query_and_return_df('get_weekly_metrics_for_user')
        return df
        
    @st.cache
    def get_tweet_metrics_for_user(self):
        df = self._run_query_and_return_df('get_tweet_metrics_for_user')
        return df
    
    @st.cache
    def get_follower_metrics_for_user(self):
        df = self._run_query_and_return_df('get_follower_metrics_for_user')
        return df
    
    @st.cache
    def get_user_dimensions(self):
        df = self._run_query_and_return_df('get_user_dimensions')
        return df

    @st.cache
    def get_cohorted_metrics_for_user(self):
        df = self._run_query_and_return_df('get_cohorted_metrics_for_user')
        return df