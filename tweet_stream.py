from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json


# Documentation
# http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
def sendRecord(tup):
    word   = tup[0]
    amount = tup[1]
    producer.send('test',key=b'lol',value=b'msg')  



if __name__ == "__main__":
    conf = SparkConf().setAppName("using foreachRDD and foreach on RDD")
    sc   = SparkContext(conf=conf)
    ssc  = StreamingContext(sc, 2)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10, 1))



    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream("localhost", 5556)
    # lines = ssc.textFileStream('./streamingData')

    # Split each line into words
    words = lines.flatMap(lambda line: line.split(" "))

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))

    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # http://spark.apache.org/docs/latest/streaming-programming-guide.html#design-patterns-for-using-foreachrdd
    wordCounts.foreachRDD(lambda rdd: rdd.foreach(sendRecord))

    # Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.pprint()

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate