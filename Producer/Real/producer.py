from json import dumps
from os import environ
import requests
import time
import json
import datetime
from datetime import datetime, timedelta
import pytz


from kafka import KafkaProducer

MAX_ENTRIES = 50
YOUR_API_KEY = environ.get("YOUR_API_KEY")
base_url = "https://api.themoviedb.org/3/"
movie_path = "movie/now_playing"
language = "en-US"


KAFKA_TOPIC = environ.get("KAFKA_TOPIC", "tmdb-movies")
KAFKA_CONFIGURATION = {
    "bootstrap_servers": environ.get("KAFKA_BROKER", "kafka1:19092").split(","),
    "key_serializer": lambda x: str.encode("" if not x else x, encoding='utf-8'),
    "value_serializer": lambda x: dumps(dict() if not x else x).encode(encoding='utf-8'),
    "reconnect_backoff_ms": int(100)
}

def send_to_kafka(data):
    try:
        connecting = True
        entries = 0
      
        while connecting and entries<MAX_ENTRIES:
            try:
                print("Configuration")
                print(KAFKA_CONFIGURATION)
                producer = KafkaProducer(**KAFKA_CONFIGURATION)
                if producer.bootstrap_connected():
                    connecting = False
            except Exception as e:
                entries +=1
                print(f"Kafka-producer connection error: {e}")
            print(f"Connecting to Kafka ({entries})...")

        if entries>=MAX_ENTRIES:
            print("Cannot connect to Kafka.")
            return

        print(f"Kafka successfullly connected. Connected to bootsrap servers.")
        # check if the string is a valid JSON object
        try:
            json_object = json.loads(data)
            print("The dictionary is a valid JSON object.")
        except ValueError:
            print("The dictionary is not a valid JSON object.")

        results = json_object.get("results", None)
        for result in results:
            message = result
            producer.send(topic=KAFKA_TOPIC, key=json_object.get("id", None), value=message)
    
        producer.flush() 
        
    except Exception as e:
        print(f"Error: {e}.")
        

def main():
    time.sleep(30)   
    counter = 1
    while True:
        try:
            movie_url = "https://api.themoviedb.org/3/movie/now_playing?api_key=7c13d311d648e2685c94dc298f05c042&language=en&page={0}".format(counter)
            # DATASET_API_LINK = movie_url
            print(movie_url)
            response = requests.get(url=movie_url)
            
            if response.ok:
                data = response.json()
                data_json = json.dumps(data)
                print("Sending data to kafka ...")
                print(data_json)
                send_to_kafka(data_json)
                counter= counter + 1
                time.sleep(20)   
            else:
                raise Exception((f"Response status code: {response.status_code}\nResponse text: {response.text}"))
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
        except Exception as e:
            print(f"OTHER ERROR:{e}")
        


if __name__ == "__main__":
    main()