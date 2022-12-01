from flask import Flask, flash, request
import pandas as pd
import time
from datetime import date, timedelta
import os
import requests

from confluent_kafka import Consumer


appFlask = Flask("Consumer_app")
appFlask.secret_key = b'nqLyLEBL2QKciWYZvAcAuawKx_dsRzQGaTA7zn66QZQ' #

URL_PRODUCER = "http://producer1:1129"

consumer_config = {
    'bootstrap.servers': "kafka1:19092",
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
            while len(Hourly_messages) < 24: # 24 messages a day
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
            yesterday = str(date.today() - timedelta(day=1)).replace("-","")

            # Data cleaning of hourly_messages here
            for hour in range(len(Hourly_messages)) :
                df = pd.read_parquet(Hourly_messages[hour])
                df['Hour'] = [hour]*len(df.index)
                df.dropna()
                df.to_parquet(f"/data/{yesterday}/Flights_{yesterday}_{hour}.parquet")

            #Daily stats
            df_hourly = pd.read_parquet(f"/data/{yesterday}/")
            df_hourly.drop_duplicates(["dest_iata","org_iata"]) 
            df_hourly.to_parquet(f"/data/{yesterday}/Flights_{yesterday}")
            

        except Exception as ex:
                print("Kafka Exception : {}", ex)
        


@appFlask.route("/live", methods=['GET'])
def live():
    if request.method == 'GET' :
        last_updated = os.path.getmtime("/data/Flights.parquet") 
        if time.time()-last_updated < 5*60 : # 5 minutes
            print("Already up to date")
            return("Already up to date")
        else :
            requests.get(URL_PRODUCER+"/live") # send request to live update in app_producer
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
                with open("/data/live/Flights.parquet", 'wb') as file:
                    file.write(message)
                print("File updated")
                flash("Success !","success")
                return (f'Updated : {time.ctime()}')
                
            except Exception as ex:
                print(ex)
                flash("Error","error")
                return str(ex)
    return ("Live update")
  
if __name__ == "__main__":
    appFlask.run(debug = True, host = "0.0.0.0", port=2000)