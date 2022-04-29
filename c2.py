from kafka import KafkaConsumer
from json import loads;
from pymongo import MongoClient

def write(message):
    client = MongoClient()
    client = MongoClient('mongodb://localhost:27017/')
    db = client.dbt
    d = db["tweets"];
    d.insert_one(message);

consumer = KafkaConsumer('CSK',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: loads(x.decode('utf-8')))
for message in consumer:
    message = message.value
    write(message)
    print('{} added to '.format(message))


