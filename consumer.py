from kafka import KafkaConsumer
from json import loads;

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('IPL',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: loads(x.decode('utf-8')))
consumer2 = KafkaConsumer('MI',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: loads(x.decode('utf-8')))

consumer3 = KafkaConsumer('CSK',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: loads(x.decode('utf-8')))
for message in consumer:
    message = message.value
    print('{} added to '.format(message))

for message in consumer2:
    message = message.value
    print('{} added to '.format(message))

for message in consumer3:
    message = message.value
    print('{} added to '.format(message))


# consume earliest available messages, don't commit offsets
KafkaConsumer(auto_offset_reset='earliest', enable_auto_commit=False)

# consume json messages
KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('ascii')))

# consume msgpack
KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
KafkaConsumer(consumer_timeout_ms=1000)

# Subscribe to a regex topic pattern
consumer = KafkaConsumer()
consumer.subscribe(pattern='^awesome.*')

# Use multiple consumers in parallel w/ 0.9 kafka brokers
# typically you would run each on a different server / process / CPU
consumer1 = KafkaConsumer('my-topic',
                          group_id='my-group',
                          bootstrap_servers='my.server.com')
