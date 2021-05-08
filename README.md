# BigData_CaseStudy
The project aims at building a data platform for real time moderation and analytics of twitter data. The implementation will utilise different big data technologies as Spark, Kafka and Hive, in addition to visualisation tools for data discovery and delivering insights

Introduction 
	This is a Big Data Case Study about to get some data from twitter to make some Sentiment Analysis on this data and send replies based on it, and to use this sentiment analysis to reply on the user’s tweet and to make some visualization about this data.
I have used “#StrangerThings” since they released the new season trailer yesterday 6/5/2021, To check the interaction on this hashtag before the release and the day of releasing  

Technologies
Let's write down the languages, tools i used, libraries and its versions. 
For example:
  •	Hdp 2.6.5
  •	Python 3.6 or 3.7
  •	Kafka 1.1.2 
  •	Pyspark 2.4.7 or 2.4.6
  •	tweepy

Project Pipeline:
   1. capture data from twitter
   2. save this data in a kafka topic
   3. consume this data using spark and make some sentiment analysis and reply on these tweets based on this sentiment analysis
   4. save data as a paruet files on hdfs and create hive table 
   5. connect powerBI using ODBC with hive tables to get data from and make the required visualization
   
Configurations used to make a virtual environment to work on:

	python3.6 -m venv ./iti41
	source iti41/bin/activate
	pip install --upgrade pip
	pip install confluent-kafka
	pip install pyspark
	pip install tweepy
	pip install --force-reinstall pyspark==2.4.6
 
Some configurations to connect with twitter API
  consumer_key = 
  consumer_secret = 
  access_token = 
  access_token_secret = 

  auth = tw.OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)
  api = tw.API(auth, wait_on_rate_limit=True)

