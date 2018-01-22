from pyspark.context import SparkContext
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import *

def process(gp, df):

    # new_df = df.agg(mean(df.col3[0]), mean(df.col3[1]), mean(df.col3[2]))
    new_df = gp.agg(min(df.col3[0]).alias("min0"), min(df.col3[1]).alias("min1"), count(df.TimeStamp).alias("count"))\
            .sort(asc("window"))

    return new_df
