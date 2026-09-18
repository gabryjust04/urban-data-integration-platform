import re
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ============================================================
# PREPARING
# ============================================================

def prepare_taxi(df: DataFrame) -> DataFrame:
    return df.withColumn("pickup_hour", F.date_trunc("hour", F.col("tpep_pickup_datetime")))


def prepare_weather(df: DataFrame) -> DataFrame:
    return df.withColumn("weather_hour", F.date_trunc("hour", F.col("observation_timestamp"))).select("weather_hour", "temp", "rhum", "prcp","coco")


def prepare_pickup_zones(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )


def prepare_dropoff_zones(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("location_id").alias("do_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )


def prepare_air_quality(df: DataFrame) -> DataFrame:
    nyc_counties = ["New York", "Kings", "Queens", "Bronx", "Richmond"]
    df = df.filter((F.col("state_name") == "New York") & F.col("county_name").isin(nyc_counties))
    df = df.withColumn("air_quality_hour", F.date_trunc("hour", F.col("observation_timestamp")))
    return df.groupBy("air_quality_hour").agg(F.avg("sample_measurement").alias("air_quality_measurement"))

# ============================================================
# WRITE
# ============================================================
def write_gold(df: DataFrame):
    df.write.format("delta").mode("overwrite").partitionBy("year","month").save("storage/gold/integrated_taxi_trips")

# ============================================================
# MAIN FUNCTION
# ============================================================

def build_integrated_taxi_trips(spark):
    print(f"\n--- Integrating integrated_taxi_trips ---")
    start = time.perf_counter()
    taxi_df = spark.read.format("delta").load("storage/silver/taxi_trips")
    weather_df = spark.read.format("delta").load("storage/silver/weather")
    air_quality_df = spark.read.format("delta").load("storage/silver/air_quality")
    zones_df = spark.read.format("delta").load("storage/silver/taxi_zones")

    taxi_df = prepare_taxi(taxi_df)
    weather_df = prepare_weather(weather_df)
    pickup_zones = prepare_pickup_zones(zones_df)
    dropoff_zones = prepare_dropoff_zones(zones_df)
    air_quality_df = prepare_air_quality(air_quality_df)

    df = taxi_df.join(dropoff_zones, on="do_location_id", how="left")
    df = df.join(pickup_zones, on="pu_location_id", how="left")
    df = df.join(weather_df, df["pickup_hour"] == weather_df["weather_hour"], "left").drop("weather_hour")
    df = df.join(air_quality_df, df["pickup_hour"] == air_quality_df["air_quality_hour"], "left").drop("air_quality_hour")

    write_gold(df)
    execution_time = time.perf_counter() - start
    print(f"Finisched integrated_taxi_trips build\nTime: {execution_time:.2f}s")