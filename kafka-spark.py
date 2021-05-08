
import findspark
findspark.init()
from json import loads
import jsonpickle
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import re
import pyarrow
from textblob import TextBlob
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import tweepy as tw

consumer_key = 'FyQTtoLzVdf7qgufWx98v390n'
consumer_secret = 'QS1bkqJK3Cu5xSMMx1Lh3wAgO2nrwfuK9ZHsEl4bd9OdOZSKfh'
access_token = '1385771330214240259-HtQ2EVontcVPEJqMbpnqAl1ffp0mCV'
access_token_secret = 'MTo3abg96QGZL3a2S6pkn7EWiG08XF2xkUd9WjcOLuuEq'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

conf = SparkConf()
conf.setAppName("SentimentAnalysis")

sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)
ssc = StreamingContext(sc, 3)

kafkaStream = KafkaUtils.createDirectStream(ssc, topics=['demo'], kafkaParams={"metadata.broker.list": 'sandbox-hdp.hortonworks.com:6667' })


def get_data(data):
    data_tweet = data[1]
    tweetstr = loads(data_tweet).encode('utf-8')
    tweet = jsonpickle.decode(tweetstr)
    
    tweet_id = tweet['tweet_id']
    user_name = tweet['user_name']
    tweet_txt = tweet['Tweet']
    source_App = tweet['source_App']
    tweet_time = tweet['tweet_time']
    user_location = tweet['user_location']
    verified = tweet['verified']

    
    Message = get_tweet_sentiment(tweet_id,tweet_txt)
    column_names = [tweet_id, tweet_txt,user_name,source_App,tweet_time,user_location,verified,Message]
    return column_names


elements_list = []
try:
    add_elements_file = open("add_elements_file.txt", "r")
    for element in add_elements_file:
        element = element.split('\n')[0]
        elements_list.append(element)
except:
    print("File Not Found")

    
def check(rdd, spark):

    if (rdd.isEmpty() == False):
        rdd = rdd.map(get_data)
        schema = StructType([StructField("Tweet_id",StringType(),True),
        StructField("tweet_txt",StringType(),True),StructField("user_name",StringType(),True),
        StructField("source_App",StringType(),True),StructField("tweet_time",StringType(),True),
        StructField("user_location",StringType(),True),StructField("verified",StringType(),True),
        StructField("Message",StringType(),True)
        ])
        
        df = spark.createDataFrame(rdd,schema)
        #df.show()

        #print(elements_list)
        list_elements = rdd.collect()
        get_id = list_elements[0][0]
        
        if get_id not in elements_list:
            #print(get_id)
            
            elements_list.append(get_id)
            df.write.mode("append").parquet('hdfs://sandbox-hdp.hortonworks.com:8020/root/case_study/demo')
            add_elements_file = open("add_elements_file.txt", "w")
            for element in elements_list:
                add_elements_file.write(element + "\n")
            add_elements_file.close()
        else:
            print("File Already Exist")
        #print(elements_list)

        
kafkaStream.foreachRDD(lambda rdd: check(rdd,spark))

def get_tweet_sentiment(tweet_id,tweet_txt):

    analysis = TextBlob(tweet_txt)
    if analysis.sentiment.polarity > 0:
        reply = "Positive"
        Message = "Positive"
    elif analysis.sentiment.polarity == 0:
        reply = "Neutral"
        Message = "Neutral"
    else:
        reply = "Negative"
        Message = "Negative"
    try:
        api.update_status(status = reply, in_reply_to_status_id = tweet_id, auto_populate_reply_metadata=True)
    except:
        print("Replied!")
    return Message

ssc.start()
ssc.awaitTermination()