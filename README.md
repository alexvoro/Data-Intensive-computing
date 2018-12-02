# Data-Intensive-computing

Data-Intensive computing course labs
===================

**Lab1:** TopTen and WordCount (Java)

Given a list of users, print out the information of the top ten users based on their reputation. In your code, each mapper reads data from HDFS and determines the top ten records of its input split and outputs them to the reduce phase. The mappers should filter their input split to the top ten records, and the reducer is responsible for the final ten and write the result in table in HBase.

**Lab2:** Basic operations of Spark (RDDs) and Spark SQL (DataFrames) (Scala)

**Lab3:** Spark Streaming application, while reading streaming data from Kafka and storing the result in Cassandra datastore (Java)

**Lab4:** Spark GraphX to process graph-based data (Scala)


--------------