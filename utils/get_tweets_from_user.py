import requests
import os
import json
import pandas as pd

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")

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

def get_user_id_from_user_name(user_name):
    user_dict = connect_to_endpoint("https://api.twitter.com/2/users/by?usernames={}".format(user_name))
    return user_dict['data'][0]['id']

def create_url(user_name):
    user_id = get_user_id_from_user_name(user_name)
    return "https://api.twitter.com/2/users/{}/tweets".format(user_id)
    

def get_params(pagination_token=None):
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
        "pagination_token": pagination_token
    }


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r


def get_100_tweets_from_user(user_name, pagination_token=None):
    url = create_url(user_name)
    params = get_params(pagination_token=pagination_token)
    json_response = connect_to_endpoint(url, params)
    # print(json.dumps(json_response, indent=4, sort_keys=True))
    return json_response

def get_full_user_timeline(user_name):
    hundo_tweets = get_100_tweets_from_user(user_name)
    next_token = hundo_tweets["meta"]["next_token"]
    full_tweet_df = pd.DataFrame(hundo_tweets['data'])
    while True:
        hundo_tweets = get_100_tweets_from_user(user_name, next_token)
        full_tweet_df = full_tweet_df.append(
            pd.DataFrame(hundo_tweets['data'])
        )
        if "next_token" not in hundo_tweets["meta"]:
            break
        else:
            next_token = hundo_tweets["meta"]["next_token"]
    return full_tweet_df
        

if __name__ == "__main__":
    get_full_user_timeline('parker_brydon')