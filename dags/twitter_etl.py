import tweepy
import pandas as pd
import json
from datetime import datetime
import requests
import s3fs
import os

def run_twitter_etl():
    access_key = "IZjylADKPCgamu29s66R9NfZj"
    access_secret = "2uYaJv5MW7uTdzka4KCToprXmkOvqOuXHOqrCmdhSUHOhvJW5D"
    consumer_key = "3094439280-zXnNxnIKXEl6M97a1IOSxsoN6twxOPOdykUq3zt"
    consumer_secret = "bCBDr4zgGCBkKGI94em2LB7DdmhJPtRv0vUnhsLwDEh23"

    # 1. Create connection between local and twitter API
    #Twitter Authentication

    auth = tweepy.OAuthHandler (access_key,access_secret)
    auth.set_access_token (consumer_key, consumer_secret)

    # Creating an API object
    api = tweepy.API(auth=auth) #Tweepy documentation - https://docs.tweepy.org/en/stable/

    # As of Feb 2023, twitter only allows posts through free account of Twitter API 
    # tweets = api.user_timeline (screen_name = '@elonmusk',
    #                             #max allowed count = 200
    #                             count = 200,
    #                             #rts = retweets
    #                             include_rts = False,
    #                             #extended tweet mode for keeping full text or else only 140 chars are extracted
    #                             tweet_mode = 'extended')


    # url = 'https://gist.githubusercontent.com/hrp/900964/raw/2bbee4c296e6b54877b537144be89f19beff75f4/twitter.json'

    # response = requests.get(url)

    # if response.status_code == 200:
    #     twitter_sample_data = response.json()
    # else:
    #     print(f"Failed to fetch data. Status code: {response.status_code}")
    #     exit()

    dag_path = os.getcwd()

    with open (f'{dag_path}/data/sample_twitter_data.json','r') as json_file:
        data = json_file.read()

    tweet = json.loads(data)

    tweet_list = []

    refined_tweet = {
        'user': tweet['user']['screen_name'],
        'text': tweet['text'],
        'favourites_count': tweet['user']['favourites_count'],
        'retweet_count': tweet['retweet_count'],
        'created_at': tweet['created_at']
    }

    tweet_list.append(refined_tweet)

    # usually an API will return JSON object and below code can be used to parse it
    # print (twitter_sample_data['user']['screen_name'])

    # for tweet in twitter_sample_data:
    #     # text = tweet._json ["full_text"]
    #     print ('type of tweet',type(tweet))
        # refined_tweet = {
        #     'user': tweet['user']['screen_name'],
        #     'text': tweet['text'],
        #     'favorite_count': tweet['favorite_count'],
        #     'retweet_count': tweet['retweet_count'],
        #     'created_at': tweet['created_at']
        # }

        
    #     tweet_list.append(refined_tweet)

    df = pd.DataFrame (tweet_list)

    AWS_ACCESS_KEY = os.getenv ("AWS_ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv ("AWS_SECRET_KEY")
    AWS_BUCKET = os.getenv ("AWS_BUCKET")

    df.to_csv(f"s3://{AWS_BUCKET}/sample_twitter_data.csv",
    index=False,
    storage_options={
        "key": AWS_ACCESS_KEY,
        "secret": AWS_SECRET_KEY
    },
    )