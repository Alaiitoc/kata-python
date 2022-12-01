from flask import Flask
from producer import AutoProducer
import Loading as loading
import os
from time import time, ctime

appFlask = Flask("Producer_app")
auto_producer = AutoProducer("auto_topic")
live_producer = AutoProducer("live_topic")

@appFlask.route("/")
def index():
    return ("Producer API :\
         /autoUpdate\
         and /live")

@appFlask.route("/autoUpdate")
def auto():
    #SENDING DATA TO KAFKA TOPIC
    while True :
        try :
            data = loading.planes()
            auto_producer.send_msg_async(data)
            return (f'Updated : {ctime(time())}')
        except Exception as ex :
            return ('Failed', ex)


@appFlask.route("/live")
def live():
    #SENDING DATA TO KAFKA TOPIC
    try :
        data = loading.planes()
        live_producer.send_msg_async(data)
        return (f'Updated : {ctime(time())}')
    except Exception as ex :
        print(ex)
        return ('Failed : ',ex)

if __name__ == "__main__":
    appFlask.run(debug = True, host = "0.0.0.0", port= os.getenv('PORT',1129))