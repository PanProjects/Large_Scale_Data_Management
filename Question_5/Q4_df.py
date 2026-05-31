import os
from time import perf_counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

HDFS_ROOT    = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER    = os.environ.get("DSML_USER", "dsml00305")
CRIME_PATH   = f"{HDFS_ROOT}/data/LA_Crime_Data"
STATION_PATH = f"{HDFS_ROOT}/data/LA_Police_Stations.csv"
RESULT_PATH  = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q4_df_result"

# Degrees-to-km scale factors at 34°N latitude (Los Angeles)
# 1° lat ≈ 111 km,  1° lon ≈ 111 × cos(34°) ≈ 92 km
KM_PER_LAT = 111.0
KM_PER_LON = 92.0

spark = SparkSession.builder.appName("Q4_DataFrame").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load crime data — keep only records with valid GPS coordinates
# LAT=0 / LON=0 means location unknown in this dataset
crimes = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv([
        f"{CRIME_PATH}/LA_Crime_Data_2010_2019.csv",
        f"{CRIME_PATH}/LA_Crime_Data_2020_2025.csv",
    ])
    .filter(F.col("LAT").isNotNull() & (F.col("LAT") != 0.0))
    .filter(F.col("LON").isNotNull() & (F.col("LON") != 0.0))
    .select("DR_NO", "LAT", "LON")
)

# Load police stations (21 rows), small enough to broadcast to every executor
stations = (
    spark.read
    .option("header", "true")
    .csv(STATION_PATH)
    .select(
        F.col("DIVISION").alias("division"),
        F.col("Y").cast(DoubleType()).alias("st_lat"),
        F.col("X").cast(DoubleType()).alias("st_lon"),
    )
    .filter(F.col("st_lat").isNotNull() & F.col("st_lon").isNotNull())
)

# Cross join crimes × stations and compute Euclidean distance in km.
# Catalyst selects BroadcastNestedLoopJoin because stations has only 21 rows (~2 KB):
# the whole table is sent to every executor so all 21 distances are computed locally,
# with no shuffle of the large crime dataset.
distances = (
    crimes.crossJoin(F.broadcast(stations))
    .withColumn(
        "distance_km",
        F.sqrt(
            F.pow((F.col("LAT") - F.col("st_lat")) * KM_PER_LAT, 2)
            + F.pow((F.col("LON") - F.col("st_lon")) * KM_PER_LON, 2)
        ),
    )
)

# Find nearest station per crime.
# F.min on a struct compares lexicographically, so the first field (distance_km)
# drives the comparison — this gives the row with the smallest distance in a
# single groupBy pass, without needing a Window function or a self-join.
nearest = (
    distances
    .groupBy("DR_NO")
    .agg(F.min(F.struct("distance_km", "division")).alias("nearest"))
    .select(
        F.col("nearest.division").alias("division"),
        F.col("nearest.distance_km").alias("distance_km"),
    )
)

# Aggregate per division: count crimes and compute average distance
result_df = (
    nearest
    .groupBy("division")
    .agg(
        F.round(F.avg("distance_km"), 3).alias("average_distance"),
        F.count("*").alias("crime_count"),
    )
    .orderBy(F.col("crime_count").desc())
    .select("division", "average_distance", "crime_count")
)

# Print the physical plan chosen by the Catalyst optimizer
print("\n=== Physical Plan ===")
result_df.explain("formatted")
print("====================\n")

print("\n=== Query 4 Results (DataFrame API) ===")
start = perf_counter()
result_df.show(30)
elapsed = perf_counter() - start
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RESULT_PATH)
print(f"Result saved to: {RESULT_PATH}")

spark.stop()
