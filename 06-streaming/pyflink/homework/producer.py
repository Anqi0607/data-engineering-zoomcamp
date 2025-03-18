import csv
import json
from kafka import KafkaProducer

from time import time

def main():
    # Create a Kafka producer
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # check if successfully connected to the server
    print(producer.bootstrap_connected())

    csv_file = './green_tripdata_2019-10.csv'  # changed to homework folder

    topic_name = 'green_trips'

    t0 = time()

    with open(csv_file, 'r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        columns_need = [
            'lpep_pickup_datetime',
            'lpep_dropoff_datetime',
            'PULocationID',
            'DOLocationID',
            'passenger_count',
            'trip_distance',
            'tip_amount'
        ]

        for row in reader:
            # Each row will be a dictionary keyed by the CSV headers
            # Send data to Kafka topic "green_trips"
            # passenger_count might be empty in csv file
            # convert empty value to null, otherwise flink will throw an error
            message = {
                key: (None if key == 'passenger_count' and row[key] == '' else row[key]) 
                for key in columns_need if key in row
                }
            producer.send(topic_name, value=message)
            print(f"Sent: {message}")

    # Make sure any remaining messages are delivered using flush()
    producer.flush()
    producer.close()

    t1 = time()
    took = t1 - t0
    print(f'took {(took):.2f} seconds')


if __name__ == "__main__":
    main()