from kafka import KafkaProducer
import json
import time

from twitter_scraper_stream import scrape

# serializing data 
def json_serializer(data):
    return json.dumps(data).encode("utf-8")

# instantiating the Producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        value_serializer = json_serializer
)

if __name__ == "__main__":
    try:
        while True:
            tweet=scrape()
            for i in tweet:
                producer.send("tweet",i)
            time.sleep(60)
    except:
        producer.close()
        print("\r", end="")
        exit(0)
