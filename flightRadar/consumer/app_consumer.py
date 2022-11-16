from flask import Flask, request
from consumer.consumer import *
import producer.Loading as loading
import pandas as pd
import time
import os


appFlask = Flask(__name__)
consumer_auto = Consumer("auto_topic")
consumer_live = Consumer("live_topic")


@appFlask.route("/autoUpdate", methods=['GET'])
def index():
    if request.method == 'GET' : 
        Hourly_messages = []
        try:
            while len(Hourly_messages) < 4: # 4 messages by hour
                # read single message at a time
                msg = consumer_auto.poll(0)
                if msg is None:
                    sleep(2)
                    continue
                if msg.error():
                    print("Error reading message : {}".format(msg.error()))
                    continue
                msg.value().append(Hourly_messages)
                consumer_auto.commit()
            consumer_auto.close()

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
        


@appFlask.route("/update", methods=['GET'])
def live():
    if request.method == 'GET' :
        last_updated = time.ctime(os.path.getmtime("data/Flights.parquet")) #change path to data
        if time.time()-last_updated < 5*60 : # 5 minutes
            pass
        else :
            request.get() # send request to live update in app_producer
            try:
                while msg == None: 
                    # read single message at a time
                    msg = consumer_live.poll(0)
                    if msg is None:
                        sleep(2)
                        continue
                    if msg.error():
                        print("Error reading message : {}".format(msg.error()))
                        continue
                    message = msg.value()
                    consumer_live.commit()
                consumer_live.close()
                with open("Flights.parquet", 'w') as file:
                    file.write(message)
                
            except Exception as ex:
                    print(ex)


if __name__ == "__main__":
    appFlask.run(debug = True, host = "0.0.0.0")