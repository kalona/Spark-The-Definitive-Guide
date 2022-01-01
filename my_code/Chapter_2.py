from pyspark.sql import SparkSession

# spark = SparkSession.builder.master("local[*]").getOrCreate()

spark = SparkSession.builder.getOrCreate()

file_path = "C:\home_work\local_github\Spark-The-Definitive-Guide\data\/flight-data\csv\/2015-summary.csv"

# COMMAND ----------


# COMMAND ----------

flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("./data/flight-data/csv/2015-summary.csv")

# COMMAND ----------

flightData2015.createOrReplaceTempView("flight_data_2015")


# COMMAND ----------

sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")

dataFrameWay = flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .count()

sqlWay.explain()
dataFrameWay.explain()


# COMMAND ----------

from pyspark.sql.functions import max, col
#
flightData2015.select(max(col("count"))).show(1)


# COMMAND ----------

maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")

maxSql.show()


# COMMAND ----------

from pyspark.sql.functions import desc

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .show()


# COMMAND ----------

flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)", "destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .explain()


# COMMAND ----------
