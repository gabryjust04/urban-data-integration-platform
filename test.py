import re
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType



def display(spark):
    df = spark.read.format("delta").load("storage/silver/air_quality")
    df.select(
        "parameter_name",
        "units_of_measure"
    ).distinct().show(
        truncate=False
    )


def benchmark(spark):
    total_start = time.perf_counter()

    t0 = time.perf_counter()
    taxi = spark.read.format("delta").load("storage/silver/taxi_trips")
    lookup = spark.read.format("delta").load("storage/silver/taxi_zones")
    print(f"Plan build: {time.perf_counter() - t0:.4f}s")

    t0 = time.perf_counter()
    taxi = taxi.join(lookup,taxi.pu_location_id == lookup.location_id)
    taxi.groupBy("borough").count().show()
    print(f"Query 1 (Count by borough): {time.perf_counter() - t0:.4f}s\n")

    t0 = time.perf_counter()
    taxi.groupBy("borough").agg(F.avg("fare_amount")).show()
    print(f"Query 2 (Avg fare by borough): {time.perf_counter() - t0:.4f}s\n")

    t0 = time.perf_counter()
    taxi = taxi.withColumn("duration_sec",F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"))
    taxi = taxi.withColumn(
        "date",
        F.date_trunc("day","tpep_pickup_datetime")
    )
    taxi.groupby("date").agg(F.avg("duration_sec").alias("avg_duration_sec")).show()

    print(f"Query 3 (Avg duration by day): {time.perf_counter() - t0:.4f}s\n")

    print(f"Total benchmark run: {time.perf_counter() - total_start:.4f}s")