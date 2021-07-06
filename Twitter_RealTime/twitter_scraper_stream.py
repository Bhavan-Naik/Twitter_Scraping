from textblob import TextBlob
import twint
import nest_asyncio 
import pandas as pd
import numpy
import time
from datetime import datetime, timedelta
nest_asyncio.apply()

limit= 10

def scrape():
    tweets={
        "date":[],
        "user":[],
        "tweet":[],
        "interactions":[]
    }
    now=datetime.now()
    since=(now-timedelta(seconds=60)).strftime('%Y-%m-%d %H:%M:%S')
    until=(now+timedelta(seconds=12)).strftime('%Y-%m-%d %H:%M:%S')
    print(now,since,until)
    
    c=twint.Config()
    c.Search="#BTC" 
    c.Lang="en"
    c.Pandas=True
    c.Show_hashtags=True
    c.Limit=10
    c.Hide_output=True
    c.Filter_retweets=True
    twint.run.Search(c)

    df=twint.storage.panda.Tweets_df
    for i in range(len(df)):
        dates=df["date"][len(df)-i-1]
        if dates<since or dates>until:
            continue
        tweets["date"].append(df["date"][len(df)-i-1])
        tweets["user"].append(df["username"][len(df)-i-1])
        tweets["tweet"].append(df["tweet"][len(df)-i-1])
        count=0
        if isinstance(df["nretweets"][len(df)-i-1], numpy.int64)==True:
            count+=int(df["nretweets"][len(df)-i-1])
        if isinstance(df["nlikes"][len(df)-i-1], numpy.int64)==True:
            count+=int(df["nlikes"][len(df)-i-1])
        if isinstance(df["nreplies"][len(df)-i-1], numpy.int64)==True:
            count+=int(df["nreplies"][len(df)-i-1])        
        tweets["interactions"].append(count)
    
    for index in range(len(tweets["date"])):
        date=tweets["date"][index]
        user=tweets["user"][index]
        tweet=tweets["tweet"][index]
        interactions=tweets["interactions"][index]
        yield {
        "date":date,
        "user":user,
        "tweet":tweet,
        "interactions":interactions
        }

scrape()
