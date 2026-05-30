import os
from time import perf_counter
from pyspark.sql import SparkSession

HDFS_ROOT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER = os.environ.get("DSML_USER", "dsml00305")
DATA_PATH = f"{HDFS_ROOT}/data/LA_Crime_Data"
RESULT_PATH = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q2_rdd_result"

spark = SparkSession.builder.appName("Q2_RDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

csv_2010 = f"{DATA_PATH}/LA_Crime_Data_2010_2019.csv"
csv_2020 = f"{DATA_PATH}/LA_Crime_Data_2020_2025.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv([csv_2010, csv_2020])
rdd = df.rdd

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

street_rdd = rdd.filter(
    lambda row: row["Premis Desc"] is not None and row["Premis Desc"].upper() == "STREET"
)

segment_rdd = (
    street_rdd
    .map(lambda row: (assign_segment(row["TIME OCC"]), 1))
    .filter(lambda kv: kv[0] is not None)
)

counts_rdd = segment_rdd.reduceByKey(lambda a, b: a + b)

print("\n=== Query 1 Results (RDD) ===")
start = perf_counter()
counts = counts_rdd.collect()
elapsed = perf_counter() - start

total = sum(cnt for _, cnt in counts)
results = sorted(
    [(seg, cnt, round(cnt / total * 100, 2)) for seg, cnt in counts],
    key=lambda x: -x[2]
)

for seg, cnt, pct in results:
    print(f"  {seg:12s}  count={cnt:8d}  pct={pct:6.2f}%")
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

result_rdd = sc.parallelize(
    ["segment,crime_count,percentage"] + [f"{seg},{cnt},{pct}" for seg, cnt, pct in results], 1
)
result_rdd.saveAsTextFile(RESULT_PATH)
print(f"Result saved to: {RESULT_PATH}")

spark.stop()
