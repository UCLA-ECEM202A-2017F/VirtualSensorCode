#!/bin/bash

# Python3 path
export PYSPARK_PYTHON=/usr/local/bin/python3

#Spark path
export SPARK_HOME=/Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7

#PySpark args (do not change unless you know what you are doing)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.1 pyspark-shell"

#set spark home
export PATH=$SPARK_HOME/bin:$PATH

export SPARK_MASTER_OPTS="-Dspark.deploy.defaultCores=1"