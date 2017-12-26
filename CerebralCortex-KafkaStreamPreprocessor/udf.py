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
    rowRDD = rdd.map(lambda w: Row(time=w["time"], value=w["value"]))
    df = spark.createDataFrame(rowRDD)
    df.select(mean(df["value"][0]), mean(df["value"][1]), mean(df["value"][2])).show()
    df.select(max(df["value"][0]), max(df["value"][1]), max(df["value"][2])).show()
    print ("===== Process Done =====")
