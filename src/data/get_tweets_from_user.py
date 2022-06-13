import json
import requests
import pandas as pd
from sqlalchemy import create_engine
import json
from decouple import config

ENGINE_PATH = f"postgresql://{config('DB_USER')}:{config('DB_PWD')}@{config('DB_HOST')}/{config('DB')}"
BEARER_TOKEN = config("BEARER_TOKEN")

engine = create_engine(ENGINE_PATH)
db_conn = engine.connect()

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


class TweetScraper():

    def __init__(self, user_name, start_time, end_time=None) -> None:
        self.user_name = user_name
        self.start_time = start_time
        self.end_time = end_time
        self.user_id = self.get_user_id_from_user_name()
        self.user_timeline_df = self._scrape_n_save_user_timeline()
        self.user_meta_data_df = self._scrape_n_save_user_meta_data()
        self._scrape_n_save_interactions_from_tweets()

    def get_user_id_from_user_name(self):
        user_dict = connect_to_endpoint("https://api.twitter.com/2/users/by?usernames={}".format(self.user_name))
        return user_dict['data'][0]['id']

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
            "tweet.fields": "attachments,author_id,context_annotations,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld",
            # "tweet.fields": "attachments,author_id,context_annotations,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,organic_metrics,promoted_metrics,public_metrics,referenced_tweets,source,text,withheld",
            "max_results": 100,
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
        user_timeline_df.to_sql(
            name='user_timelines',
            con=db_conn,
            if_exists='append',
            schema='twitter'
        )
        return user_timeline_df

    def _scrape_n_save_user_meta_data(self) -> pd.DataFrame:
        params = {
            "user.fields": "created_at,description,public_metrics,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld"
        }
        url = f"https://api.twitter.com/2/users?ids={self.user_id}"
        json_response = connect_to_endpoint(url, params)
        raw_user_meta_data_df = pd.DataFrame(json_response)
        user_meta_data_df = clean_df_for_postgres(raw_user_meta_data_df)
        user_meta_data_df.to_sql(
            name='user_meta_data',
            con=db_conn,
            if_exists='replace',
            schema='twitter'
        )
        return user_meta_data_df

    def _scrape_n_save_interactions_from_tweets(self) -> int:
        for ix, row in self.user_timeline_df.iterrows():
            metrics = eval(row['public_metrics'])
            if metrics['like_count'] > 0:
                likers_df = self._get_likers_for_tweet(row['id'])
                if likers_df is not None:
                    likers_df.to_sql(
                        name='tweets_liked',
                        con=db_conn,
                        if_exists='append',
                        schema='twitter'
                    )
            if metrics['quote_count'] > 0:
                quoters_df = self._get_quoters_for_tweet(row['id'])
                if quoters_df is not None:
                    quoters_df.to_sql(
                        name='tweets_quoted',
                        con=db_conn,
                        if_exists='append',
                        schema='twitter'
                    )
            if metrics['reply_count'] > 0:
                repliers_df = self._get_repliers_for_tweet(row['id'])
                if repliers_df is not None:
                    repliers_df.to_sql(
                        name='tweets_replied_to',
                        con=db_conn,
                        if_exists='append',
                        schema='twitter'
                    )
            if metrics['retweet_count'] > 0:
                rters_df = self._get_rters_for_tweet(row['id'])
                if rters_df is not None:
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
            "tweet.fields": "attachments,author_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,source,text,withheld",
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
    scraper = TweetScraper('parker_brydon', start_time='2022-06-01T00:00:00Z')
    df = scraper.scrape_n_save_user_timeline()
    print(df.shape)
