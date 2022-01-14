
# Basic Structured Operations

# For dbconnect with Databricks
import os
print(os.environ['SPARK_HOME'])

# Set path to local spark home to work locally
os.environ['SPARK_HOME']="C:\\Users\\A625517\\spark"

# Remove $SPARK_HOME to work on Databricks cluster
os.environ.pop('SPARK_HOME')

# get the directory for that this notebook is in
path = os.getcwd()
print("path is",path)

# path = os.path.abspath(os.path.join(raw_path, 'data'))
# print("path is", path)

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Chapter 5").getOrCreate()


file = "\\data\\flight-data\\json\\2015-summary.json"

df = spark.read.format("json").load(path + file)

df.printSchema()

df.schema

# Manual schema

from pyspark.sql.types import StructField, StructType, StringType, LongType

manualSchema = StructType(
    [
        StructField("DEST_COUNTRY_NAME", StringType(), True),
        StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
        StructField("count", LongType(), False, metadata={"hello":"world"})
    ]
)

df = spark.read.format("json").schema(manualSchema).load(path + file)

df.printSchema()

df.show()

# Columns

from pyspark.sql.functions import col, column

col("someColName")
column("someColumnName")

df.select(col("count")).show()

# Expressions

from pyspark.sql.functions import expr

expr("(((someCol + 5) * 200) - 6) < otherCol")

df.columns

# Records and Rows

