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

KM_PER_LAT = 111.0
KM_PER_LON = 92.0

spark = SparkSession.builder.appName("Q4_DataFrame").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

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

nearest = (
    distances
    .groupBy("DR_NO")
    .agg(F.min(F.struct("distance_km", "division")).alias("nearest"))
    .select(
        F.col("nearest.division").alias("division"),
        F.col("nearest.distance_km").alias("distance_km"),
    )
)

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
