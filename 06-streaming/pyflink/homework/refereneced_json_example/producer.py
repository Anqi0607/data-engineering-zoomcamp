import csv
import json
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from ride import Ride
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC

#1，2，5，6，7，8，12

class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)


if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    }
    producer = JsonProducer(props=config)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH)
    producer.publish_rides(topic=KAFKA_TOPIC, messages=rides)
