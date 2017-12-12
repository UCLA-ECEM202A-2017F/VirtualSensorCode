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

/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-all.sh
#set master
export SPARK_IDENT_STRING=master1
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh -h 127.0.0.1 -p 8081
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin//start-slave.sh spark://127.0.0.1:8081

export SPARK_IDENT_STRING=master2
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh -h 127.0.0.1 -p 8083
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin//start-slave.sh spark://127.0.0.1:8083

spark-submit --conf spark.cores.max=1 --master spark://127.0.0.1:8081 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 test.py "/Users/Shengfei/Desktop/cerebralcortex/data/"
