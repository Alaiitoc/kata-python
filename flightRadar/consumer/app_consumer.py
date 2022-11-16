from flask import Flask, flash, request
import pandas as pd
import time
import os
import requests

from confluent_kafka import Consumer


appFlask = Flask("Consumer_app")
appFlask.secret_key = b'nqLyLEBL2QKciWYZvAcAuawKx_dsRzQGaTA7zn66QZQ'

URL_CONSUMER = "http://10.110.1.162:1129"

consumer_config = {
    'bootstrap.servers': "localhost:9092",
    'group.id': 'consumer-1',
    'auto.offset.reset': 'largest',
    'enable.auto.commit': 'false',
    'max.poll.interval.ms': '86400000'
}

@appFlask.route("/")
def index():
    return ("Consumer API :\
         /autoUpdate\
         and /live")

@appFlask.route("/autoUpdate", methods=['GET'])
def auto():
    if request.method == 'GET' : 
        Hourly_messages = []
        consumer = Consumer(consumer_config)
        consumer.subscribe(["auto_topic"])
        try:
            while len(Hourly_messages) < 4: # 4 messages by hour
                # read single message at a time
                msg = consumer.poll(0)
                if msg is None:
                    time.sleep(2)
                    continue
                if msg.error():
                    print("Error reading message : {}".format(msg.error()))
                    continue
                msg.value().append(Hourly_messages)
                consumer.commit()
            consumer.close()

            # Data cleaning of hourly_messages here
            df0 = pd.read_parquet(Hourly_messages[0])
            df1 = pd.read_parquet(Hourly_messages[1])
            df2 = pd.read_parquet(Hourly_messages[2])
            df3 = pd.read_parquet(Hourly_messages[3])

            df_hourly = pd.concat([df0,df1,df2,df3], ignore_index=True)

            df_hourly.drop_duplicates(["dest_iata","org_iata"]) 
            # I'll need to think about this a bit more in order to have stats that make sense but for now it's a start

        except Exception as ex:
                print("Kafka Exception : {}", ex)
        


@appFlask.route("/live", methods=['GET'])
def live():
    if request.method == 'GET' :
        last_updated = os.path.getmtime("../data/Flights.parquet") # WARNING data path
        if time.time()-last_updated < 5*60 : # 5 minutes
            print("Already up to date")
            return("Already up to date")
        else :
            requests.get(URL_CONSUMER+"/live") # send request to live update in app_producer
            consumer = Consumer(consumer_config)
            consumer.subscribe(["live_topic"])
            try:
                msg = None
                while msg == None: 
                    # read single message at a time
                    print("Listening")
                    msg = consumer.poll(0)
                    if msg is None:
                        time.sleep(10)
                        continue
                    if msg.error():
                        print("Error reading message : {}".format(msg.error()))
                        continue
                    message = msg.value()
                    consumer.commit()
                consumer.close()
                with open("../data/Flights.parquet", 'wb') as file:
                    file.write(message)
                print("File updated")
                flash("Success !","success")
                return (f'Updated : {time.time()}')
                
            except Exception as ex:
                print(ex)
                flash("Error","error")
                return str(ex)
    return ("Live update")

if __name__ == "__main__":
    appFlask.run(debug = True, host = "0.0.0.0", port=2000)