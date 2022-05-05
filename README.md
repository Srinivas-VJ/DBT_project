#Project 
Problem offered under our Database Technologies elective to Stream data from twitter to Apache Spark to perform SparkSQL queries to Apache Kafka and store values in a database of our choice 

#Code Structure
*receivetweets.py - Streams tweets based on topic you've entered.
*stream.py - Socket streaming with Apache Spark and defining kafka producers.
*c1,c2,c3,c4.py - Kafka consumers and storing messages to mongodb.

#Instructions
*Install the Kafka zip file from the community page after which you must extract the file and open multiple terminals in this directory.
*In the first terminal you must start the zookeeper , In order to do so run the following command.
  ```
 bin/zookeeper-server-start.sh config/zookeeper.properties
 
 ```
*On another terminal start the kafka bootstrap server 
  ```
 bin/kafka-server-start.sh config/server.properties
 
 ```
*And finally install mongo db and create a database called dbt and a collection called tweets and keep the mongo server running on your terminal.
 
*Now head over to your repo open 3 terminals and run the python files in order of receivetweets.py, c1/c2/c3/c4.py and stream.py . you can see how we were able to mimic the architechture expected of us.
 
