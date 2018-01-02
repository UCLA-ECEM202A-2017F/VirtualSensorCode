from pyspark.context import SparkContext
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import *

def process(rdd):
    print ("===== Process Start =====")
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

    # myRdd = data.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    # rowRdd = myRdd.map(lambda w: Row(word=w[0], Count=w[1]))
    # df = spark.createDataFrame(rowRdd)
    #### End of example
    # # Convert RDD[String] to RDD[Row] to DataFrame
    # rowRdd = rdd.map(lambda w: Row(word=w))
    # wordsDataFrame = spark.createDataFrame(rowRdd)
    # # Creates a temporary view using the DataFrame
    # wordsDataFrame.createOrReplaceTempView("words")
    # # Do word count on table using SQL and print it
    # wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")

# integrate function in json format
# stop from other teminal
# // output in a file with stat records
# study kafka offset
# acceleration vs words count (as sensor input)
