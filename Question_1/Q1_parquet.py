import os
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_ROOT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER = os.environ.get("DSML_USER", "dsml00305")
PARQUET_PATH = f"{HDFS_ROOT}/user/{DSML_USER}/data/LA_Crime_Parquet"
RESULT_PATH = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q1_parquet_result"

spark = SparkSession.builder.appName("Q1_Parquet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(PARQUET_PATH)

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

print("\n=== Query 1 Results (Parquet) ===")
start = perf_counter()
result_df.show()
elapsed = perf_counter() - start
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RESULT_PATH)
print(f"Result saved to: {RESULT_PATH}")

spark.stop()
