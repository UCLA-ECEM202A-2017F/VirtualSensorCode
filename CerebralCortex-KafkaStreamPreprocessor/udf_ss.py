from pyspark.context import SparkContext
from pyspark.sql import Row, SparkSession, SQLContext
from pyspark.sql.functions import *

def process(df):
    new_df = df.agg(mean(df.col3[0]), mean(df.col3[1]), mean(df.col3[2]))
    return new_df
