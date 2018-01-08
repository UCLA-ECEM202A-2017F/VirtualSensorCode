from pyspark.context import SparkContext
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import *

def process(df):

    # new_df = df.agg(mean(df.col3[0]), mean(df.col3[1]), mean(df.col3[2]))
    new_df = df.groupBy(window("TimeStamp", "10 seconds", "10 seconds", "5 seconds"))\
            .agg(mean(df.col3[0]), mean(df.col3[1]), mean(df.col3[2]), count(df.TimeStamp))\
            .sort(asc("window"))
    return new_df
