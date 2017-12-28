from pyspark.context import SparkContext
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import *

def process(rdd):
    spark = SparkSession.builder.appName("Cerebral-Cortex").getOrCreate()
    # df2 = spark.createDataFrame(rdd)
    # df2.show(5)
    # df = rdd.toDF(["time","id","value"])
    # df.show()
    # df.select(max(df["value"]), mean(df["value"])).show()
    # rowRDD = rdd.map(lambda w: Row(time=w["time"], value=w["Value"]))
    # df = spark.createDataFrame(rowRDD)
    rowRDD = rdd.map(lambda w: Row(TimeStamp=w[0], Offset=w[1], Value=w[2]))
    df = spark.createDataFrame(rowRDD)
    # df = rdd.toDF(["time","offset","value"]) # put sensorid into the df (future)

    df.select(mean(df["Value"][0]), mean(df["Value"][1]), mean(df["Value"][2])).show()
    df.select(max(df["Value"][0]), max(df["Value"][1]), max(df["Value"][2])).show()
    print ("===== Process Done =====")
