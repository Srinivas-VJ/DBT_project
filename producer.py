from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'], api_version=(0, 10, 1))

# Asynchronous by default
future = producer.send('test', b'raw_bytes')

# Block for 'synchronous' sends
try:
    record_metadata = future.get(timeout=10)
except KafkaError:
    # Decide what to do if produce request failed...
    log.exception()
    pass

# Successful result returns assigned partition and offset
print(record_metadata.topic)
print(record_metadata.partition)
print(record_metadata.offset)

# produce keyed messages to enable hashed partitioning
producer.send('my-topic', key=b'foo', value=b'bar')

# encode objects via msgpack
# producer = KafkaProducer(
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# producer.send('msgpack-topic', {'key': 'value'})

# # produce json messages
# producer = KafkaProducer(
#     value_serializer=lambda m: json.dumps(m).encode('ascii'))
# producer.send('', {'key': 'value'})

# produce asynchronously
for _ in range(100):
    producer.send('lol', b'msg')


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception


# produce asynchronously with callbacks
producer.send(
    'test', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)
