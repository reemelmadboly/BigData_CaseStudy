import os

import jsonpickle as jsonpickle
import tweepy as tw
import pandas as pd
from time import sleep
from json import dumps
from kafka import KafkaProducer

consumer_key = '///////////////'
consumer_secret = '///////////////'
access_token = '///////////////'
access_token_secret = '///////////////'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

search_words = "#reem_demo"
date_since = "2021-2-1"
#data_until = "2021-5-7"


producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'], value_serializer=lambda x: dumps(x).encode('utf-8'))

tweets_list = []
dict_data = {}

while True:
    tweets = tw.Cursor(api.search,q=search_words,lang="en", since=date_since).items()  
                       
    for tweet in tweets:
       
        if tweet not in tweets_list:
            text = tweet.text
            tweet_id = tweet.id_str
            user_name = tweet.user.screen_name
            source_App = tweet.source
            tweet_time = str(tweet.created_at)
            user_location = tweet.user.location
            verified = str(tweet.user.verified)
            
            dict_data = {'Tweet': text,'user_name':user_name,'tweet_id':tweet_id,'source_App':source_App,'tweet_time':tweet_time,'user_location':user_location,'verified':verified }
            print(dict_data)
            
            tweets_list.append(tweet)


            row = jsonpickle.encode(dict_data)
            producer.send('demo', value=row)
            sleep(3)
        else:
            sleep(60) 

'''
retweeted = str(tweet.retweeted)
name = tweet.user.name
followers_count = str(tweet.user.followers_count)
following_count = str(tweet.user.friends_count)
account_CreationDT = str(tweet.user.created_at)
favourites_count = str(tweet.user.favourites_count)
'''
