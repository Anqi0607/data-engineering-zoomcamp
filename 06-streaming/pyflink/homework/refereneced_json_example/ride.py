from typing import List, Dict
from decimal import Decimal
from datetime import datetime


class Ride:

    # pass进一个list（即从csv读到的一个row），用list中每个元素的值作为Ride class object的attribute的值
    def __init__(self, arr: List[str]):
        self.tpep_pickup_datetime = datetime.strptime(arr[0], "%Y-%m-%d %H:%M:%S"),
        self.tpep_dropoff_datetime = datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S"),
        self.pu_location_id = int(arr[2])
        self.do_location_id = int(arr[3])
        self.passenger_count = int(arr[4])
        self.trip_distance = Decimal(arr[5])
        self.tip_amount = Decimal(arr[6])

    @classmethod
    #用于consume message的时候，把json转换回Ride class instances
    #因为message在发给Kafka时变成了json（dict格式）
    def from_dict(cls, d: Dict):
        return cls(arr=[
            d['tpep_pickup_datetime'][0],
            d['tpep_dropoff_datetime'][0],
            d['pu_location_id'],
            d['do_location_id'],
            d['passenger_count'],
            d['trip_distance'],
            d['tip_amount'],
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'

