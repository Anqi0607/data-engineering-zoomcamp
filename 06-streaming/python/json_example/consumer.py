from typing import Dict, List
from json import loads
from kafka import KafkaConsumer

from ride import Ride
from settings import BOOTSTRAP_SERVERS, KAFKA_TOPIC


class JsonConsumer:
    def __init__(self, props: Dict):
        self.consumer = KafkaConsumer(**props)

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics)
        print('Consuming from Kafka started')
        print('Available topics to consume: ', self.consumer.subscription())
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                # 在用poll()接收消息时，无法处理其他事件（比如按下 Ctrl + C 中断程序），所以限制poll()的超时为1秒
                # 所以每 1 秒就会检查一次消息，即使没有消息，程序也会在 1 秒后继续执行其他代码，从而能够更快地响应 SIGINT 信号（即用户的中断请求）。
                message = self.consumer.poll(1.0)
                if message is None or message == {}:
                    continue
                for message_key, message_value in message.items():
                    for msg_val in message_value:
                        print(msg_val.key, msg_val.value)
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == '__main__':
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'auto_offset_reset': 'earliest',
        #让consumer记住已经被consume的message
        'enable_auto_commit': True,
        #把key（pu_location_id）从byte变回integer
        'key_deserializer': lambda key: int(key.decode('utf-8')),
        #将byte用utf-8 decode为json，再从json变回Ride class instances
        'value_deserializer': lambda x: loads(x.decode('utf-8'), object_hook=lambda d: Ride.from_dict(d)),
        'group_id': 'consumer.group.id.json-example.2',
    }

    json_consumer = JsonConsumer(props=config)
    json_consumer.consume_from_kafka(topics=[KAFKA_TOPIC])
