import os
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

HDFS_ROOT   = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER   = os.environ.get("DSML_USER", "dsml00305")
GEOJSON     = f"{HDFS_ROOT}/data/LA_Census_Blocks_2020.geojson"
INCOME_CSV  = f"{HDFS_ROOT}/data/LA_income_2021.csv"
RESULT_PATH = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q3_df_result"

spark = SparkSession.builder.appName("Q3_DataFrame").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

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

# Aggregate population and housing units per ZIP code
zip_stats = (
    blocks
    .select(
        F.trim(F.col("ZCTA20")).alias("zip"),
        F.col("POP20").cast(LongType()).alias("population"),
        F.col("HOUSING20").cast(LongType()).alias("housing_units"),
    )
    .filter(F.col("zip").isNotNull())
    .filter(F.col("population").isNotNull() & (F.col("population") > 0))
    .groupBy("zip")
    .agg(
        F.sum("population").alias("total_pop"),
        F.sum("housing_units").alias("total_hu"),
    )
)

# Load income CSV (semicolon-delimited), strip $ and commas from income values
income_raw = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .csv(INCOME_CSV)
)
cols = income_raw.columns
income = (
    income_raw
    .select(
        F.trim(F.col(cols[0])).alias("zip"),
        F.regexp_replace(F.trim(F.col(cols[2])), r"[\$,\s]", "").cast(DoubleType()).alias("median_income"),
    )
    .filter(F.col("zip").isNotNull() & F.col("median_income").isNotNull())
)

# Join on ZIP and compute per-capita income
# income dataset is small (arround 200 rows) so broadcast it to avoid a shuffle join
result_df = (
    zip_stats
    .join(F.broadcast(income), "zip")
    .withColumn(
        "per_capita_income",
        F.round(F.col("median_income") * (F.col("total_hu") / F.col("total_pop")), 2),
    )
    .select("zip", "total_pop", "total_hu", "median_income", "per_capita_income")
    .orderBy("zip")
)

print("\n=== Query 3 Results (DataFrame API) ===")
start = perf_counter()
result_df.show(20)
elapsed = perf_counter() - start
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RESULT_PATH)
print(f"Result saved to: {RESULT_PATH}")

spark.stop()
