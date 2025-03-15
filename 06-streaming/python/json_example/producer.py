import csv
import json
from typing import List, Dict
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from ride import Ride
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC


#继承 KafkaProducer class而不是组合
#因为下面需要自定义value_serializer
class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
        # props为producer的properties，它接收一个dict作为参数
        self.producer = KafkaProducer(**props)

    @staticmethod
    # staticmethod for utility function
    # no need to create class object or access class data
    def read_records(resource_path: str):
        records = []
        with open(resource_path, 'r') as f:
            #把csv里的row一行一行读成一个list
            reader = csv.reader(f)
            #跳过了第一行的header，reader里就会从第二行开始iterate
            header = next(reader)  # skip the header row
            for row in reader:
                #每个row变成Ride class instance，再append到一个record这个list里
                #所以record是一个 list of Ride instances
                records.append(Ride(arr=row))
        return records

    def publish_rides(self, topic: str, messages: List[Ride]):
        for i, ride in enumerate(messages):
            try:
                partition = i % 2
                record = self.producer.send(topic=topic, key=ride.pu_location_id, value=ride, partition=partition)
                print('Record {} successfully produced at offset {}'.format(ride.pu_location_id, record.get().offset))
            except KafkaTimeoutError as e:
                print(e.__str__())


if __name__ == '__main__':
    # Config Should match with the KafkaProducer expectation
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),
        #x.__dict__: 先读每个Ride instance的属性dict
        #再把它转换为json，default=str确保像 Decimal 或 datetime 类型能被正确转换为json
        #encode('utf-8'):编码为 UTF-8 byte，才符合Kafka对message的格式要求
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    }
    producer = JsonProducer(props=config)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH)
    producer.publish_rides(topic=KAFKA_TOPIC, messages=rides)
