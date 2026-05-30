import os
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

HDFS_ROOT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER = os.environ.get("DSML_USER", "dsml00305")
DATA_PATH = f"{HDFS_ROOT}/data/LA_Crime_Data"
RESULT_PATH = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q3_df_result"

spark = SparkSession.builder.appName("Q3_DataFrame").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

csv_2010 = f"{DATA_PATH}/LA_Crime_Data_2010_2019.csv"
csv_2020 = f"{DATA_PATH}/LA_Crime_Data_2020_2025.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv([csv_2010, csv_2020])

monthly_counts = (
    df
    .withColumn("year",  F.year(F.to_timestamp("DATE OCC", "yyyy MMM dd hh:mm:ss a")))
    .withColumn("month", F.month(F.to_timestamp("DATE OCC", "yyyy MMM dd hh:mm:ss a")))
    .filter(F.col("year").isNotNull())
    .groupBy("year", "month")
    .agg(F.count("*").alias("crime_total"))
)

year_window = Window.partitionBy("year").orderBy(F.col("crime_total").desc())

result_df = (
    monthly_counts
    .withColumn("ranking", F.rank().over(year_window))
    .filter(F.col("ranking") <= 3)
    .orderBy("year", F.col("crime_total").desc())
    .select("year", "month", "crime_total", "ranking")
)

print("\n=== Query 2 Results (DataFrame API) ===")
start = perf_counter()
result_df.show(50)
elapsed = perf_counter() - start
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RESULT_PATH)
print(f"Result saved to: {RESULT_PATH}")

spark.stop()
