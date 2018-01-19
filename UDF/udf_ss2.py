from pyspark.context import SparkContext
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import *

def process(gp, df):

    # new_df = df.agg(mean(df.col3[0]), mean(df.col3[1]), mean(df.col3[2]))
    new_df = gp.agg(max(df.col3[0]).alias("max0"), max(df.col3[1]).alias("max1"), max(df.col3[2]).alias("max2"), count(df.TimeStamp).alias("count"))\
            .sort(asc("window"))

    return new_df
