from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
from json import dumps



from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split




# Create a local StreamingContext with two working thread and batch interval of 1 second
# sc = SparkContext("local[2]", "NetworkWordCount")
# ssc = StreamingContext(sc, 1)
# lines = ssc.socketTextStream("localhost", 5556)
# words = lines.flatMap(lambda line: line.split(" "))
# pairs = words.map(lambda word: (word, 1))
# wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# # Print the first ten elements of each RDD generated in this DStream to the console
# wordCounts.pprint()
def send_to_kafka(rows):
    #producer = KafkaProducer(bootstrap_servers = util.get_broker_metadata())
        word = rows[0]
        count = rows[1]
        print(rows)
        print(type(word))
        producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'], api_version=(0, 10, 1), value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

        # Asynchronous by defaul

        # Successful result returns assigned partition and offset
        # t = bytes(count);
        v= {rows[0]:rows[1]}

        producer.send('test',value=v)
            #producer.flush()


if __name__ == "__main__":
    
    sc = SparkContext("local[2]", "NetworkWordCount")
    ssc = StreamingContext(sc, 1)
    lines = ssc.socketTextStream("localhost", 5555)
    words = lines.flatMap(lambda line: line.split(" "))
    pairs = words.map(lambda word: (word, 1))
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)
    wordCounts.foreachRDD(lambda rdd: rdd.foreach(send_to_kafka))
    ssc.start()             # Start the computation
    ssc.awaitTermination()
    # encode objects via msgpack
    # producer = KafkaProducer(
    #     value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    # producer.send('msgpack-topic', {'key': 'value'})

    # # produce json messages
    # producer = KafkaProducer(
    #     value_serializer=lambda m: json.dumps(m).encode('ascii'))
    # producer.send('', {'key': 'value'})

    # produce asynchronously
    # for _ in range(100):
    #     producer.send('lol', b'msg')


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
        # handle exception


    # # produce asynchronously with callbacks
    # producer.send(
    #     'test', b'raw_bytes').add_callback(on_send_success).add_errback(on_send_error)

    # # block until all async messages are sent
    # producer.flush()

    # # configure multiple retries
    # producer = KafkaProducer(retries=5)
