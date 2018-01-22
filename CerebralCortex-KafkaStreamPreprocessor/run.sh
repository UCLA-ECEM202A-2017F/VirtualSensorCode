#!/bin/bash

# Python3 path
export PYSPARK_PYTHON=/usr/local/bin/python3

#Spark path
export SPARK_HOME=/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7

#PySpark args (do not change unless you know what you are doing)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 pyspark-shell"

#set spark home
export PATH=$SPARK_HOME/bin:$PATH

export SPARK_MASTER_OPTS="-Dspark.deploy.defaultCores=1"

spark-submit --master spark://127.0.0.1:8081 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 ss_session.py /Users/Shengfei/Desktop/cerebralcortex/data/ /Users/Shengfei/Desktop/cerebralcortex/Archive/ user_query0.json 2
# spark-submit --master spark://127.0.0.1:8082 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 ss_session.py /Users/Shengfei/Desktop/cerebralcortex/data/ /Users/Shengfei/Desktop/cerebralcortex/Archive/ user_query1.json 3
# spark-submit --master spark://127.0.0.1:8084 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 ss_session.py /Users/Shengfei/Desktop/cerebralcortex/data/ /Users/Shengfei/Desktop/cerebralcortex/Archive/ user_query2.json 4


#spark-submit --conf spark.cores.max=1 --master spark://127.0.0.1:8083 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 main.py /Users/Shengfei/Desktop/cerebralcortex/data/
