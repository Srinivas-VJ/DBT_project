from pyspark import SparkContext
from pyspark.streaming import StreamingContext
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

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
# Successful result returns assigned partition and offset
print(record_metadata.topic)
print(record_metadata.partition)
print(record_metadata.offset)
def send(tup):
    producer.send(
    'test',bytes(tup[0]))


# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream("localhost", 5556)
words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.foreachRDD(lambda r: r.foreach(send))

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()
ssc.start()             # Start the computation
ssc.awaitTermination()






# produce keyed messages to enable hashed partitioning
#producer.send('my-topic', key=b'foo', value=b'bar')

# encode objects via msgpack
# producer = KafkaProducer(
#     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# producer.send('msgpack-topic', {'key': 'value'})

# # produce json messages
# producer = KafkaProducer(
#     value_serializer=lambda m: json.dumps(m).encode('ascii'))
# producer.send('', {'key': 'value'})

# produce asynchronously
#for _ in range(100):
   # producer.send('test', b'msg')



    # handle exception


# produce asynchronously with callbacks


# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)