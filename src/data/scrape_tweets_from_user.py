from datetime import datetime
import json
import requests
import pandas as pd
from settings import db_conn
from src.data.update_app_data import update_user_scrape_status_to_success
import json
from decouple import config
import coloredlogs, logging
import time
logger = logging.getLogger(__name__)
coloredlogs.install(level=config('LOG_LEVEL'))

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {config('BEARER_TOKEN')}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r


def connect_to_endpoint(url, params=''):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    if response.status_code == 429:
        logger.warn('429 status code, sleeping for 15 minutes')
        time.sleep(60*15)
        response = requests.request("GET", url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def clean_df_for_postgres(in_df):
    out_df = in_df.copy()
    for col in out_df.columns:
        if (
            isinstance(out_df[col].iloc[0], dict) or
            isinstance(out_df[col].iloc[0], list)
        ):
            out_df[col] = out_df[col].apply(json.dumps).astype(str)
        else:
            out_df[col] = out_df[col].astype(str)
    return out_df

def get_user_id_from_user_name(user_name):
    user_dict = connect_to_endpoint("https://api.twitter.com/2/users/by?usernames={}".format(user_name))
    return user_dict['data'][0]['id']

class TweetScraper():

    def __init__(self, user_name, start_time=None, end_time=None) -> None:
        self.user_name = user_name
        self.start_time = start_time
        self.end_time = end_time
        logger.info(f'Getting user_id for {user_name}')
        self.user_id = get_user_id_from_user_name(user_name)
        logger.info(f'Getting user metadata for {user_name}')
        self._scrape_n_save_user_meta_data(self.user_id)
        logger.info(f'Getting followers for {user_name}')
        self._scrape_n_save_followers_for_user()
        logger.info(f'Getting timeline for {user_name}')
        self.user_timeline_df = self._scrape_n_save_user_timeline()
        logger.info(f'Getting tweet engagements for {user_name}')
        self._scrape_n_save_interactions_from_tweets()
        update_user_scrape_status_to_success(self.user_id)

    def create_url(self):
        return "https://api.twitter.com/2/users/{}/tweets".format(self.user_id)

    def get_params(self, pagination_token=None):
        # Tweet fields are adjustable.
        # Options include:
        # attachments, author_id, context_annotations,
        # conversation_id, created_at, entities, geo, id,
        # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
        # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
        # source, text, and withheld
        return {
            "tweet.fields": "attachments,author_id,context_annotations,created_at,entities,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld",            "max_results": 100,
            "pagination_token": pagination_token,
            "start_time": self.start_time,
            "end_time": self.end_time
        }

    def get_user_timeline(self) -> pd.DataFrame:
        return self.user_timeline_df

    def get_user_meta_data(self) -> pd.DataFrame:
        return self.user_meta_data_df

    def _get_100_tweets_from_user(self, pagination_token=None):
        url = self.create_url()
        params = self.get_params(pagination_token=pagination_token)
        json_response = connect_to_endpoint(url, params)
        return json_response

    def _scrape_n_save_user_timeline(self) -> pd.DataFrame:
        hundo_tweets = self._get_100_tweets_from_user()
        raw_user_timeline_df = pd.DataFrame(hundo_tweets['data'])
        while True:
            if "next_token" not in hundo_tweets["meta"]:
                break
            else:
                next_token = hundo_tweets["meta"]["next_token"]
                hundo_tweets = self._get_100_tweets_from_user(next_token)
                raw_user_timeline_df = raw_user_timeline_df.append(
                    pd.DataFrame(hundo_tweets['data'])
                )
        user_timeline_df = clean_df_for_postgres(raw_user_timeline_df)
        user_timeline_df['row_created_at'] = str(datetime.now())
        user_timeline_df.to_sql(
            name='user_timelines',
            con=db_conn,
            if_exists='append',
            schema='twitter'
        )
        return user_timeline_df
    

    def _scrape_n_save_followers_for_user(self):
        params = {
            "user.fields": "created_at,description,public_metrics,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld"
        }
        url = f"https://api.twitter.com/2/users/{self.user_id}/followers"
        json_response = connect_to_endpoint(url, params)
        raw_users_followers_df = pd.DataFrame(json_response['data'])
        users_followers_df = clean_df_for_postgres(raw_users_followers_df)
        users_followers_df['author_user_id'] = self.user_id
        users_followers_df['row_created_at'] = str(datetime.now())
        users_followers_df.to_sql(
            name='users_followers',
            con=db_conn,
            if_exists='append',
            schema='twitter'
        )
        return 200


    def _scrape_n_save_user_meta_data(self, user_id):
        params = {
            "user.fields": "created_at,description,public_metrics,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld"
        }
        url = f"https://api.twitter.com/2/users?ids={user_id}"
        json_response = connect_to_endpoint(url, params)
        raw_user_meta_data_df = pd.DataFrame(json_response['data'])
        user_meta_data_df = clean_df_for_postgres(raw_user_meta_data_df)
        user_meta_data_df['row_created_at'] = str(datetime.now())
        user_meta_data_df.to_sql(
            name='user_meta_data',
            con=db_conn,
            if_exists='append',
            schema='twitter'
        )
        return 200

    def _scrape_n_save_interactions_from_tweets(self) -> int:
        for ix, row in self.user_timeline_df.iterrows():
            metrics = eval(row['public_metrics'])
            if not (
                row['in_reply_to_user_id'] == 'nan'
                or row['in_reply_to_user_id'] is None
            ):
                self._scrape_n_save_user_meta_data(
                    row['in_reply_to_user_id']
                )
            if metrics['like_count'] > 0:
                likers_df = self._get_likers_for_tweet(row['id'])
                if likers_df is not None:
                    likers_df['tweet_id'] = row['id']
                    likers_df['tweet_created_at'] = row['created_at']
                    likers_df['author_id'] = row['author_id']
                    likers_df['row_created_at'] = str(datetime.now())
                    likers_df.to_sql(
                        name='tweets_liked',
                        con=db_conn,
                        if_exists='append',
                        schema='twitter'
                    )
            if metrics['quote_count'] > 0:
                quoters_df = self._get_quoters_for_tweet(row['id'])
                if quoters_df is not None:
                    quoters_df['tweet_id'] = row['id']
                    quoters_df['tweet_created_at'] = row['created_at']
                    quoters_df['author_id'] = row['author_id']
                    quoters_df['row_created_at'] = str(datetime.now())
                    quoters_df.to_sql(
                        name='tweets_quoted',
                        con=db_conn,
                        if_exists='append',
                        schema='twitter'
                    )
            if metrics['reply_count'] > 0:
                repliers_df = self._get_repliers_for_tweet(row['id'])
                if repliers_df is not None:
                    repliers_df['tweet_id'] = row['id']
                    repliers_df['tweet_created_at'] = row['created_at']
                    repliers_df['author_id'] = row['author_id']
                    repliers_df['row_created_at'] = str(datetime.now())
                    repliers_df.to_sql(
                        name='tweets_replied_to',
                        con=db_conn,
                        if_exists='append',
                        schema='twitter'
                    )
            if metrics['retweet_count'] > 0:
                rters_df = self._get_rters_for_tweet(row['id'])
                if rters_df is not None:
                    rters_df['tweet_id'] = row['id']
                    rters_df['tweet_created_at'] = row['created_at']
                    rters_df['author_id'] = row['author_id']
                    rters_df['row_created_at'] = str(datetime.now())
                    rters_df.to_sql(
                        name='tweets_retweeted',
                        con=db_conn,
                        if_exists='append',
                        schema='twitter'
                    )
        return 200

    def _get_likers_for_tweet(self, tweet_id):
        url = f"https://api.twitter.com/2/tweets/{tweet_id}/liking_users"
        json_response = connect_to_endpoint(url)
        meta = json_response['meta']
        if meta['result_count'] > 0:
            data = clean_df_for_postgres(pd.DataFrame(json_response['data']))
            # TODO: handle cases where there's more than 100 of these (meta has a next_token for pagination)
            return data
        else:
            return None

    def _get_quoters_for_tweet(self, tweet_id):
        url = f"https://api.twitter.com/2/tweets/{tweet_id}/quote_tweets"
        json_response = connect_to_endpoint(url)
        meta = json_response['meta']
        if meta['result_count'] > 0:
            data = clean_df_for_postgres(pd.DataFrame(json_response['data']))
            # TODO: handle cases where there's more than 100 of these (meta has a next_token for pagination)
            return data
        else:
            return None

    def _get_rters_for_tweet(self, tweet_id):
        url = f"https://api.twitter.com/2/tweets/{tweet_id}/retweeted_by"
        json_response = connect_to_endpoint(url)
        meta = json_response['meta']
        if meta['result_count'] > 0:
            data = clean_df_for_postgres(pd.DataFrame(json_response['data']))
            # TODO: handle cases where there's more than 100 of these (meta has a next_token for pagination)
            return data
        else:
            return None

    def _get_repliers_for_tweet(self, tweet_id):
        url = f"https://api.twitter.com/2/tweets/search/recent?query=conversation_id:{tweet_id}"
        params = {
            "user.fields": "created_at,id",
            "tweet.fields": "author_id,created_at,entities,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld",
        }
        json_response = connect_to_endpoint(url, params)
        meta = json_response['meta']
        if meta['result_count'] > 0: # TODO: I think this is 0 bc were looking at the conversation id => only see replies when this was the first tweet
            data = clean_df_for_postgres(pd.DataFrame(json_response['data']))
            # TODO: handle cases where there's more than 100 of these (meta has a next_token for pagination)
            return data
        else:
            return None
            

if __name__ == "__main__":
    # TODO: make this script executable to run over all the users, for the last day?
        # what if we need more than a day?
    scraper = TweetScraper('parker_brydon', start_time='2022-06-01T00:00:00Z')
    df = scraper.scrape_n_save_user_timeline()
    print(df.shape)
