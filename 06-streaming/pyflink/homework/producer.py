import json

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()

# check if connected to server
print("Connected:", producer.bootstrap_connected())

# check partitions of the topic 
partitions = producer.partitions_for('green_trips')
print("Partitions for my_topic:", partitions)
