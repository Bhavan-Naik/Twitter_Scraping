from kafka import KafkaConsumer
import json
import os
import datetime as dt
from pymongo import MongoClient 

try:
    consumer = KafkaConsumer(
            "tweet",
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            group_id="twitter_consumer"
        )
        
    time_script_run = dt.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')  
    filename="{}_historical_output.json".format(time_script_run)

    myclient = MongoClient("mongodb+srv://<username>:<password>@<mongo_database>") #This will be available in your MongoDB 
    db = myclient["Twitter_Historical"]
    Collection = db[filename]

    if __name__ == "__main__":
        print('Starting the Consumer...')
        print('Data-Scraping will take some time...')
        for msg in consumer:        
            new_tweet = {"$set":json.loads(msg.value)}
            Collection.update_one(json.loads(msg.value), new_tweet, upsert=True)
            #Collection.insert_one(json.loads(msg.value))
            print("Tweet = {}".format(json.loads(msg.value)))
        consumer.close()
except:
    consumer.close()
    print("\r", end="")
    exit(0)
