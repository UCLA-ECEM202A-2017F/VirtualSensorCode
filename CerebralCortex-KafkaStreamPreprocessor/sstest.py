import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
from importlib import import_module

if __name__ == "__main__":

    module_cus = import_module("udf")
    reload(module_cus)
    method = getattr(module_cus, "process")

    # windowSize = int(sys.argv[3])
    windowSize = 5
    slideSize = 2
    # slideSize = int(sys.argv[4]) if (len(sys.argv) == 5) else windowSize
    # <slide duration> must be less than or equal to <window duration>", file=sys.stderr)
    windowDuration = '{} seconds'.format(windowSize)
    slideDuration = '{} seconds'.format(slideSize)

    spark = SparkSession\
        .builder\
        .appName("StructuredNetworkWordCountWindowed")\
        .getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to host:port
    lines = spark\
        .readStream\
        .format('text')\
        .load("./data")

    lines2 = spark\
        .readStream\
        .format('text')\
        .load("./data2")


    def ttl(col):
        return list(eval(col))

    def udf1(col):
        return col.split(', ', 2)

    # def average(col):
    #     return mean(col)

    sp = udf(udf1, ArrayType(StringType()))
    myttl = udf(ttl, ArrayType(DoubleType()))

    df = lines.withColumn("TimeStamp", sp(lines.value).getItem(0)) \
        .withColumn("Offset", sp(lines.value).getItem(1)) \
        .withColumn("col3", sp(lines.value).getItem(2))

    df = df.withColumn("TimeStamp", df.TimeStamp.cast("long")) \
        .withColumn("Offset", df.Offset.cast("long")) \
        .withColumn("col3", myttl(df.col3)).drop("value")

    # df4 = df.select(myttl(df.col3).alias('col3'))

    # windowedCounts = df.groupBy("col3").count()
    windowedCounts = df.select(method(df.col3))


    # === Not valid ===
    # rdd = windowedCounts.rdd.take(1)
    # windowedCounts = spark.createDataFrame(rdd)

    # Split the lines into words, retaining timestamps
    # split() splits each line into an array, and explode() turns the array into multiple rows

    # words = lines.select(
    #     explode(split(lines.value, ' ')).alias('word'),
    # )

    # words = lines.select(explode(split(lines.value, ', ')).alias('word'),)
    # words2 = lines2.select(explode(split(lines2.value, ', ')).alias('word'),)
    # words = lines.select(explode(split(lines.value, ', ')).alias('word'),)
    #
    # windowedCounts = words2.union(words).groupBy("word").count()
    # words.take(1)

    # Start running the query that prints the windowed word counts to the console
    query = windowedCounts\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .option('truncate', 'false') \
        .trigger(processingTime="25 seconds").start()

    # Group the data by window and word and compute the count of each group
    # windowedCounts = words.groupBy("word").count()
    #
    # query = windowedCounts\
    #     .writeStream\
    #     .outputMode('complete')\
    #     .format('console')\
    #     .option('truncate', 'false') \
    #     .trigger(processingTime="5 seconds").start()

    query.awaitTermination()
