from sqlalchemy import create_engine
from decouple import config
import pandas as pd
import logging

ENGINE_PATH = f"postgresql://{config('DB_USER')}:{config('DB_PWD')}@{config('DB_HOST')}/{config('DB')}"
BEARER_TOKEN = config("BEARER_TOKEN")

engine = create_engine(ENGINE_PATH)
db_conn = engine.connect()



class UserData():

    def __init__(self, user_id) -> None:
        self.user_id = user_id
    
    def _run_query_and_return_df(self, query_filename: str):
        with open(f'./src/data/sql_queries/{query}.sql', 'r') as f:
            query = f.read().format(user_id=self.user_id)
        logging.info(f'Query: {query_filename} \n{query}')
        df = pd.read_sql(query, db_conn, schema=config('DB_SCHEMA'))
        return df

    def get_weekly_metrics_for_user(self):
        df = self._run_query_and_return_df('get_weekly_metrics_for_user')
        return df
        
    def get_tweet_metrics_for_user(self):
        df = self._run_query_and_return_df('get_tweet_metrics_for_user')
        return df
    
    def get_follower_metrics_for_user(self):
        df = self._run_query_and_return_df('get_follower_metrics_for_user')
        return df
    
    def get_user_dimensions(self):
        df = self._run_query_and_return_df('get_user_dimensions')
        return df
