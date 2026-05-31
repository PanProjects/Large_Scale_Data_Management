import os
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType

HDFS_ROOT   = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER   = os.environ.get("DSML_USER", "dsml00305")
GEOJSON     = f"{HDFS_ROOT}/data/LA_Census_Blocks_2020.geojson"
INCOME_CSV  = f"{HDFS_ROOT}/data/LA_income_2021.csv"
OUTPUT_BASE = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q6_Q3_join"

spark = (
    SparkSession.builder
    .appName("Q6_Q3_JoinStrategies")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

raw = (
    spark.read
    .option("multiLine", "true")
    .json(GEOJSON)
    .selectExpr("explode(features) as feat")
    .select("feat.*")
)
prop_cols = raw.schema["properties"].dataType.fieldNames()
blocks = raw.select([F.col(f"properties.{c}").alias(c) for c in prop_cols])

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

income_raw = spark.read.option("header", "true").option("sep", ";").csv(INCOME_CSV)
cols = income_raw.columns
income = (
    income_raw
    .select(
        F.trim(F.col(cols[0])).alias("zip"),
        F.regexp_replace(F.trim(F.col(cols[2])), r"[\$,\s]", "")
         .cast(DoubleType()).alias("median_income"),
    )
    .filter(F.col("zip").isNotNull() & F.col("median_income").isNotNull())
)

zip_stats.cache()
zip_stats.count()

timings = {}
for hint in ("BROADCAST", "MERGE", "SHUFFLE_HASH", "SHUFFLE_REPLICATE_NL"):
    if hint == "BROADCAST":
        joined = zip_stats.join(income.hint("broadcast"), "zip")
    elif hint == "MERGE":
        joined = zip_stats.join(income.hint("merge"), "zip")
    elif hint == "SHUFFLE_HASH":
        joined = zip_stats.join(income.hint("shuffle_hash"), "zip")
    else:
        joined = zip_stats.join(income.hint("shuffle_replicate_nl"), "zip")

    result = (
        joined
        .withColumn(
            "per_capita_income",
            F.round(F.col("median_income") * (F.col("total_hu") / F.col("total_pop")), 2),
        )
        .select("zip", "total_pop", "total_hu", "median_income", "per_capita_income")
        .orderBy("zip")
    )

    print(f"\n{'='*60}")
    print(f"  Join strategy: {hint}")
    print(f"{'='*60}")
    result.explain("formatted")

    start = perf_counter()
    result.write.mode("overwrite").option("header", "true").csv(f"{OUTPUT_BASE}_{hint}")
    elapsed = perf_counter() - start

    timings[hint] = elapsed
    print(f"QUERY_ELAPSED_SECONDS_{hint}={elapsed:.3f}")

print(f"\n{'='*60}")
print("  Query 3 – Join Strategy Comparison")
print(f"{'='*60}")
print(f"  {'Strategy':<25}  {'Time (sec)':>10}")
for h, t in sorted(timings.items(), key=lambda x: x[1]):
    print(f"  {h:<25}  {t:>10.3f}")
print(f"{'='*60}")

spark.stop()
