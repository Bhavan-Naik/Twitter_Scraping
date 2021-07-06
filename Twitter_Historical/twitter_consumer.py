from kafka import KafkaConsumer
import json
import os
import datetime as dt
from pymongo import MongoClient 

CHECK_FOLDER = os.path.isdir("twitter_json")
if not CHECK_FOLDER:
    os.makedirs(MYDIR)

try:
    consumer = KafkaConsumer(
            "tweet",
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            group_id="twitter_consumer"
        )
        
    time_script_run = dt.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')  
    filename="{}_historical_output.json".format(time_script_run)

    myclient = MongoClient("mongodb+srv://bazman:bhavan2000@twitter.fxec6.mongodb.net/test") 
    db = myclient["Twitter_Historical"]
    Collection = db[filename]

    with open(filename,'w') as outfile:
        if __name__ == "__main__":
            print('Starting the Consumer...')
            print('Data-Scraping will take some time...')
            for msg in consumer:
                json.dump(json.loads(msg.value), outfile)
                outfile.write('\n')
                new_tweet = {"$set":json.loads(msg.value)}
                Collection.update_one(json.loads(msg.value), new_tweet, upsert=True)
                #Collection.insert_one(json.loads(msg.value))
                print("Tweet = {}".format(json.loads(msg.value)))
            consumer.close()
except:
    consumer.close()
    print("\r", end="")
    exit(0)
