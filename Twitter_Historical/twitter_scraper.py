from textblob import TextBlob
import twint
import nest_asyncio 
import pandas as pd
import numpy
nest_asyncio.apply()

#Output format
def scrape():
    tweets={
        "date":[],
        "user":[],
        "tweet":[],
        "interactions":[]
    }

    #Search parameters
    c=twint.Config()
    c.Search="#IPL"
    #c.Username= "elonmusk"  
    c.Lang="en"
    c.Pandas=True
    c.Since="2021-05-04"
    c.Until="2021-05-05"
    c.Show_hashtags=True
    #c.Limit=10
    c.Hide_output=True
    c.Filter_retweets=True
    twint.run.Search(c)

    #Extracting only the necessary fields 
    df=twint.storage.panda.Tweets_df
    for i in range(len(df["tweet"])):
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
        
    for index in range(len(df["tweet"])):
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
