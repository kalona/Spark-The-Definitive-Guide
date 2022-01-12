from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("Chapter 4").getOrCreate()

df = spark.range(500).toDF("number")

df.select(df["number"] + 10).show()