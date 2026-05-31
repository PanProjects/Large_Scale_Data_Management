import os
import re
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

HDFS_ROOT   = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER   = os.environ.get("DSML_USER", "dsml00305")
GEOJSON     = f"{HDFS_ROOT}/data/LA_Census_Blocks_2020.geojson"
INCOME_CSV  = f"{HDFS_ROOT}/data/LA_income_2021.csv"
RESULT_PATH = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q3_rdd_result"

spark = SparkSession.builder.appName("Q3_RDD").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Load census blocks GeoJSON, explode features and flatten properties
raw = (
    spark.read
    .option("multiLine", "true")
    .json(GEOJSON)
    .selectExpr("explode(features) as feat")
    .select("feat.*")
)
prop_cols = raw.schema["properties"].dataType.fieldNames()
blocks = raw.select([F.col(f"properties.{c}").alias(c) for c in prop_cols])

# Drop to RDD: each element becomes (zip, (population, housing_units))
census_rdd = (
    blocks.select("ZCTA20", "POP20", "HOUSING20").rdd
    .filter(lambda r: r["ZCTA20"] is not None)
    .filter(lambda r: r["POP20"] is not None and int(r["POP20"]) > 0)
    .map(lambda r: (
        r["ZCTA20"].strip(),
        (int(r["POP20"]), int(r["HOUSING20"]) if r["HOUSING20"] is not None else 0),
    ))
)

# Sum population and housing units per ZIP code
zip_stats_rdd = census_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# Load income CSV using DataFrame reader (handles header and delimiter), then drop to RDD
income_raw = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(INCOME_CSV)
)
cols = income_raw.columns
zip_col    = cols[0]
income_col = cols[2]


def parse_income(row):
    zip_val = row[zip_col].strip() if row[zip_col] else None
    raw_inc = row[income_col] if row[income_col] else None
    if zip_val is None or raw_inc is None:
        return None
    cleaned = re.sub(r"[\$,\s]", "", raw_inc)
    try:
        return (zip_val, float(cleaned))
    except ValueError:
        return None


income_rdd = income_raw.rdd.map(parse_income).filter(lambda x: x is not None)

# Collect income into a dict and broadcast it — avoids a shuffle join on the large RDD
income_dict = dict(income_rdd.collect())
income_bc = sc.broadcast(income_dict)

# Map each ZIP's stats to a result row: (zip, total_pop, total_hu, median_income, per_capita)
result_rdd = (
    zip_stats_rdd
    .map(lambda kv: (kv[0], kv[1][0], kv[1][1], income_bc.value.get(kv[0])))
    .filter(lambda x: x[3] is not None)
    .map(lambda x: (x[0], x[1], x[2], x[3], round(x[3] * (x[2] / x[1]), 2)))
    .sortBy(lambda x: x[0])
)

print("\n=== Query 3 Results (RDD API) ===")
start = perf_counter()
results = result_rdd.collect()
elapsed = perf_counter() - start

for row in results[:20]:
    print(f"  ZIP={row[0]}  pop={row[1]:,}  hu={row[2]:,}  median_income=${row[3]:,.2f}  per_capita=${row[4]:,.2f}")
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
fs.delete(sc._jvm.org.apache.hadoop.fs.Path(RESULT_PATH), True)

header = sc.parallelize(["zip,total_pop,total_hu,median_income,per_capita_income"])
rows   = sc.parallelize([f"{r[0]},{r[1]},{r[2]},{r[3]},{r[4]}" for r in results])
header.union(rows).saveAsTextFile(RESULT_PATH)
print(f"Result saved to: {RESULT_PATH}")

spark.stop()
