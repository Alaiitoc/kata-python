from flask import Flask
from producer.producer import *
import producer.Loading as loading
import time


appFlask = Flask(__name__)
auto_producer = AutoProducer("auto_topic")
current_producer = AutoProducer("live_topic")


@appFlask.route("/autoUpdate")
def index():
    #SENDING DATA TO KAFKA TOPIC
    while True :
        try :
            data = loading.planes()
            auto_producer.send_msg_async(data)
            return time.time()
        except :
            return 'Failed'


@appFlask.route("/update")
def update():
    #SENDING DATA TO KAFKA TOPIC
    while True :
        try :
            data = loading.planes()
            message = data
            current_producer.send_msg_async(message)
            return time.time()
        except :
            return 'Failed'


if __name__ == "__main__":
    appFlask.run(debug = True, host = "0.0.0.0")