import os
from time import perf_counter
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_ROOT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER = os.environ.get("DSML_USER", "dsml00305")
DATA_PATH = f"{HDFS_ROOT}/data/LA_Crime_Data"
RESULT_PATH = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q1_csv_result"

spark = SparkSession.builder.appName("Q1_CSV").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

csv_2010 = f"{DATA_PATH}/LA_Crime_Data_2010_2019.csv"
csv_2020 = f"{DATA_PATH}/LA_Crime_Data_2020_2025.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv([csv_2010, csv_2020])

street_df = df.filter(F.upper(F.col("Premis Desc")) == "STREET")

segmented = street_df.withColumn(
    "segment",
    F.when((F.col("TIME OCC") >= 500)  & (F.col("TIME OCC") <= 1159), "Morning")
     .when((F.col("TIME OCC") >= 1200) & (F.col("TIME OCC") <= 1659), "Afternoon")
     .when((F.col("TIME OCC") >= 1700) & (F.col("TIME OCC") <= 2059), "Evening")
     .when((F.col("TIME OCC") >= 2100) | (F.col("TIME OCC") <= 459),  "Night")
     .otherwise(None)
).filter(F.col("segment").isNotNull())

total = segmented.count()

result_df = (
    segmented.groupBy("segment")
    .agg(F.count("*").alias("crime_count"))
    .withColumn("percentage", F.round((F.col("crime_count") / total) * 100, 2))
    .orderBy(F.col("percentage").desc())
    .select("segment", "crime_count", "percentage")
)

print("\n=== Query 1 Results (CSV) ===")
start = perf_counter()
result_df.show()
elapsed = perf_counter() - start
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RESULT_PATH)
print(f"Result saved to: {RESULT_PATH}")

spark.stop()
