import json
import requests
import os
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from decouple import config
engine = create_engine(ENGINE_PATH)
db_conn = engine.connect()

ENGINE_PATH = f"postgresql://{config('DB_USER')}:{config('DB_PWD')}@{config('DB_HOST')}/{config('DB')}"
BEARER_TOKEN = config("BEARER_TOKEN")

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r


def connect_to_endpoint(url, params=''):
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


class TweetScraper():

    def __init__(self, user_name, start_time, end_time=None) -> None:
        self.user_name = user_name
        self.start_time = start_time
        self.end_time = end_time

    def get_user_id_from_user_name(self):
        user_dict = connect_to_endpoint("https://api.twitter.com/2/users/by?usernames={}".format(self.user_name))
        return user_dict['data'][0]['id']

    def create_url(self):
        user_id = self.get_user_id_from_user_name()
        return "https://api.twitter.com/2/users/{}/tweets".format(user_id)

    def get_params(self, pagination_token=None):
        # Tweet fields are adjustable.
        # Options include:
        # attachments, author_id, context_annotations,
        # conversation_id, created_at, entities, geo, id,
        # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
        # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
        # source, text, and withheld
        return {
            "tweet.fields": "attachments,author_id,context_annotations,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld",
            # "tweet.fields": "attachments,author_id,context_annotations,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,organic_metrics,promoted_metrics,public_metrics,referenced_tweets,source,text,withheld",
            "max_results": 100,
            "pagination_token": pagination_token,
            "start_time": self.start_time,
            "end_time": self.end_time
        }

    def get_100_tweets_from_user(self, pagination_token=None):
        url = self.create_url()
        params = self.get_params(pagination_token=pagination_token)
        json_response = connect_to_endpoint(url, params)
        return json_response

    def get_user_timeline(self):
        hundo_tweets = self.get_100_tweets_from_user()
        self.user_timeline_df = pd.DataFrame(hundo_tweets['data'])
        while True:
            if "next_token" not in hundo_tweets["meta"]:
                break
            else:
                next_token = hundo_tweets["meta"]["next_token"]
                hundo_tweets = self.get_100_tweets_from_user(next_token)
                self.user_timeline_df = self.user_timeline_df.append(
                    pd.DataFrame(hundo_tweets['data'])
                )
        self.user_timeline_df.to_sql(
            name='raw_user_timelines',
            con=db_conn,
            if_exists='append'
        )
        return 200
    
    def get_user_meta_data(self, user_id):
        params = {
            "user.fields": "created_at,description,public_metrics,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld"
        }
        url = f"https://api.twitter.com/2/users?ids={user_id}"
        json_response = connect_to_endpoint(url, params)
        return pd.DataFrame(json_response)

    def get_likers_for_tweet(self, tweet_id):
        url = f"https://api.twitter.com/2/tweets/{tweet_id}/liking_users"
        json_response = connect_to_endpoint(url)
        return pd.DataFrame(json_response)

    def get_quoters_for_tweets(self, tweet_id):
        url = f"https://api.twitter.com/2/tweets/{tweet_id}/quote_tweets"
        json_response = connect_to_endpoint(url)
        return pd.DataFrame(json_response)
    
    def get_rters_for_tweets(self, tweet_id):
        url = f"https://api.twitter.com/2/tweets/{tweet_id}/retweeted_by"
        json_response = connect_to_endpoint(url)
        return pd.DataFrame(json_response)
    
    def get_repliers_for_tweet(self, tweet_id):
        url = f"https://api.twitter.com/2/search/recent?query=conversation_id:{tweet_id}"
        params = {
            "user.fields": "created_at,user_name",
            "tweet.fields": "attachments,author_id,context_annotations,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld",
        }
        json_response = connect_to_endpoint(url, params)
        return pd.DataFrame(json_response)

    def get_interactions_from_tweets(self, db_conn):
        for ix, row in self.user_timeline_df.iterrows():
            metrics = row['organic_metrics']
            if metrics['like_count'] > 0:
                likers_df = self.get_likers_for_tweet(row['id'])
                likers_df.to_sql(
                    name='raw_like_transaction_facts',
                    con=db_conn,
                    if_exists='append'
                )
            if metrics['quote_count'] > 0:
                quoters_df = self.get_quoters_for_tweet(row['id'])
                quoters_df.to_sql(
                    name='raw_quote_transaction_facts',
                    con=db_conn,
                    if_exists='append'
                )
            if metrics['reply_count'] > 0:
                repliers_df = self.get_repliers_for_tweet(row['id'])
                repliers_df.to_sql(
                    name='raw_reply_transaction_facts',
                    con=db_conn,
                    if_exists='append'
                )
            if metrics['retweet_count'] > 0:
                rters_df = self.get_rters_for_tweet(row['id'])
                rters_df.to_sql(
                    name='raw_rt_transaction_facts',
                    con=db_conn,
                    if_exists='append'
                )

if __name__ == "__main__":
    scraper = TweetScraper('parker_brydon', start_time='2022-06-01T00:00:00Z')
    df = scraper.get_user_timeline()
    print(df.shape)