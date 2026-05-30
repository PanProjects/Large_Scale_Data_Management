import os
from time import perf_counter
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

HDFS_ROOT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER = os.environ.get("DSML_USER", "dsml00305")
DATA_PATH = f"{HDFS_ROOT}/data/LA_Crime_Data"
RESULT_PATH = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q2_df_udf_result"

spark = SparkSession.builder.appName("Q2_DataFrame_UDF").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

csv_2010 = f"{DATA_PATH}/LA_Crime_Data_2010_2019.csv"
csv_2020 = f"{DATA_PATH}/LA_Crime_Data_2020_2025.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv([csv_2010, csv_2020])

street_df = df.filter(F.upper(F.col("Premis Desc")) == "STREET")

def assign_segment(time_occ):
    if time_occ is None:
        return None
    if 500 <= time_occ <= 1159:
        return "Morning"
    if 1200 <= time_occ <= 1659:
        return "Afternoon"
    if 1700 <= time_occ <= 2059:
        return "Evening"
    if time_occ >= 2100 or time_occ <= 459:
        return "Night"
    return None

segment_udf = F.udf(assign_segment, StringType())

segmented = street_df.withColumn(
    "segment", segment_udf(F.col("TIME OCC"))
).filter(F.col("segment").isNotNull())

total = segmented.count()

result_df = (
    segmented.groupBy("segment")
    .agg(F.count("*").alias("crime_count"))
    .withColumn("percentage", F.round((F.col("crime_count") / total) * 100, 2))
    .orderBy(F.col("percentage").desc())
    .select("segment", "crime_count", "percentage")
)

print("\n=== Query 1 Results (DataFrame - with UDF) ===")
start = perf_counter()
result_df.show()
elapsed = perf_counter() - start
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RESULT_PATH)
print(f"Result saved to: {RESULT_PATH}")

spark.stop()
