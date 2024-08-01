import tweepy
import pandas as pd
import time
import requests
from datetime import datetime


def run_twitter_etl():
    
    # export 'BEARER_TOKEN'='<your_bearer_token>'
    bearer_token = "" 

    client = tweepy.Client(bearer_token, wait_on_rate_limit=True)

    username = "elonmusk"

    url = f"https://api.twitter.com/2/users/by/username/{username}"

    headers = {"Authorization": f"Bearer {bearer_token}"}

    response = requests.get(url, headers=headers)
    user_data = response.json()

    user_id = user_data['data']['id']

    timeline_url = f"https://api.twitter.com/2/users/{user_id}/tweets"

    params = {
        "max_results": 100,  # Maximum number of tweets per request (100 is the maximum allowed)
        "tweet.fields": "created_at",  #tweet creation timestamp
    }

    # Send the request to Twitter API v2 to fetch user timeline
    timeline_response = requests.get(timeline_url, headers=headers, params=params)
    print(timeline_response)
    print(timeline_response.json())
    timeline_data = timeline_response.json()
    #print(timeline_data)


    data = pd.DataFrame()
    # Print the tweets
    for tweet in timeline_data['data']:
        #print(tweet)
        raw_data = pd.json_normalize(tweet)
        data = pd.concat([data, raw_data])

    #print(data)
    data = data[['id', 'created_at', 'text']]
    timestr = datetime.now().strftime("%Y%m%d-%H%M%S")
    data.to_csv("s3://abhi13-twitter-airflow-etl/tweet_data"+".csv", index=False) #s3 destination


#run_twitter_etl()

