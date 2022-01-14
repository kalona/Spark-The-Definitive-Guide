# For dbconnect with Databricks
import os
print(os.environ['SPARK_HOME'])

# Set path to local spark home to work locally
os.environ['SPARK_HOME']="C:\\Users\\A625517\\spark"

# Remove $SPARK_HOME to work on Databricks cluster
os.environ.pop('SPARK_HOME')


from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Chapter 4").getOrCreate()

spark.version

df = spark.range(500).toDF("number")

df.select(df["number"] + 10).show()

spark.range(2).collect()

from pyspark.sql.types import *

b = ByteType()

d = DecimalType()

