from confluent_kafka import Consumer
from time import sleep


class AutoConsumer:
    broker = "localhost:9092"
    group_id = "consumer-1"

    def __init__(self,topic):
        self.topic = topic

    def start_listener(self,topic):
        consumer_config = {
            'bootstrap.servers': self.broker,
            'group.id': self.group_id,
            'auto.offset.reset': 'largest',
            'enable.auto.commit': 'false',
            'max.poll.interval.ms': '86400000'
        }

        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])
        return consumer
        