from pyspark.context import SparkContext
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import *

def process(gp, df):

    # new_df = df.agg(mean(df.col3[0]), mean(df.col3[1]), mean(df.col3[2]))
    # new_df = df.withWatermark("TimeStamp", "30 seconds")\
    #         .groupBy(window("TimeStamp", "10 seconds", "10 seconds", "6 seconds"))

    new_df = gp.agg(mean(df.col3[0]).alias("avg0"), mean(df.col3[1]).alias("avg1"), mean(df.col3[2]).alias("avg2"), count(df.TimeStamp).alias("count"))\
            .sort(asc("window"))

    return new_df
