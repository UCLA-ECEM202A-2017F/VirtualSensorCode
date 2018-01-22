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

export SPARK_IDENT_STRING=master1
sudo sh /Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-master.sh
sudo sh /Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/stop-slave.sh
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh -h 127.0.0.1 -p 8081
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin//start-slave.sh spark://127.0.0.1:8081

export SPARK_IDENT_STRING=master2
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh -h 127.0.0.1 -p 8083
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin//start-slave.sh spark://127.0.0.1:8083

export SPARK_IDENT_STRING=master3
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh -h 127.0.0.1 -p 8085
/Users/Shengfei/Desktop/Spark-Archive/spark-2.2.0-bin-hadoop2.7/sbin//start-slave.sh spark://127.0.0.1:8085


# export SPARK_IDENT_STRING=mster4
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/stop-master.sh
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/stop-slave.sh
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh -h 127.0.0.1 -p 8080
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/start-slave.sh spark://127.0.0.1:8080

# export SPARK_IDENT_STRING=mster5
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/stop-master.sh
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/stop-slave.sh
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh -h 127.0.0.1 -p 8082
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/start-slave.sh spark://127.0.0.1:8082

# export SPARK_IDENT_STRING=mster6
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/stop-master.sh
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/stop-slave.sh
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/start-master.sh -h 127.0.0.1 -p 8084
# sudo sh /Users/jingxianxu/Desktop/cerebralcortex/spark-2.2.0-bin-hadoop2.7/sbin/start-slave.sh spark://127.0.0.1:8084
