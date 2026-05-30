import os
from time import perf_counter

from pyspark.sql import SparkSession

HDFS_ROOT = "hdfs://hdfs-namenode.default.svc.cluster.local:9000"
DSML_USER = os.environ.get("DSML_USER", "dsml00305")
DATA_PATH = f"{HDFS_ROOT}/data/LA_Crime_Data"
RESULT_PATH = f"{HDFS_ROOT}/user/{DSML_USER}/results/Q3_sql_result"

spark = SparkSession.builder.appName("Q3_SQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

csv_2010 = f"{DATA_PATH}/LA_Crime_Data_2010_2019.csv"
csv_2020 = f"{DATA_PATH}/LA_Crime_Data_2020_2025.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv([csv_2010, csv_2020])

df.createOrReplaceTempView("crimes")

result_df = spark.sql("""
    WITH monthly AS (
        SELECT
            YEAR(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a'))  AS year,
            MONTH(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS month,
            COUNT(*) AS crime_total
        FROM crimes
        WHERE TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a') IS NOT NULL
        GROUP BY
            YEAR(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')),
            MONTH(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a'))
    ),
    ranked AS (
        SELECT
            year,
            month,
            crime_total,
            RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
        FROM monthly
    )
    SELECT year, month, crime_total, ranking
    FROM ranked
    WHERE ranking <= 3
    ORDER BY year ASC, crime_total DESC
""")

print("\n=== Query 2 Results (Spark SQL) ===")
start = perf_counter()
result_df.show(50)
elapsed = perf_counter() - start
print(f"QUERY_ELAPSED_SECONDS={elapsed:.3f}")

result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(RESULT_PATH)
print(f"Result saved to: {RESULT_PATH}")

spark.stop()
