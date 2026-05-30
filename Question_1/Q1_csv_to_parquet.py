import os
from time import perf_counter
from pyspark.sql import SparkSession

HDFS_ROOT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DATA_PATH = f"{HDFS_ROOT}/data/LA_Crime_Data"
OUTPUT_PATH = f"{HDFS_ROOT}/user/{os.environ.get('DSML_USER', 'dsml00305')}/data/LA_Crime_Parquet"

spark = SparkSession.builder.appName("Q1_CSV_to_Parquet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

csv_2010 = f"{DATA_PATH}/LA_Crime_Data_2010_2019.csv"
csv_2020 = f"{DATA_PATH}/LA_Crime_Data_2020_2025.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv([csv_2010, csv_2020])

start = perf_counter()
df.write.mode("overwrite").parquet(OUTPUT_PATH)
elapsed = perf_counter() - start
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")
print(f"Parquet written to: {OUTPUT_PATH}")
spark.stop()