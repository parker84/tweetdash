from tracemalloc import start
import requests
import os
import json
import pandas as pd

bearer_token = os.environ.get("BEARER_TOKEN")


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
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
        full_tweet_df = pd.DataFrame(hundo_tweets['data'])
        while True:
            if "next_token" not in hundo_tweets["meta"]:
                break
            else:
                next_token = hundo_tweets["meta"]["next_token"]
                hundo_tweets = self.get_100_tweets_from_user(next_token)
                full_tweet_df = full_tweet_df.append(
                    pd.DataFrame(hundo_tweets['data'])
                )
        return full_tweet_df



if __name__ == "__main__":
    scraper = TweetScraper('parker_brydon', start_time='2022-06-01T00:00:00Z')
    df = scraper.get_user_timeline()
    print(df.shape)