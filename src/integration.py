import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


GOLD_PATH = "storage/gold/integrated_taxi_trips"


# ============================================================
# PREPARING
# ============================================================

def prepare_taxi(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("pickup_hour", F.date_trunc("hour", F.col("tpep_pickup_datetime")))
        .withColumn("year", F.year("tpep_pickup_datetime"))
        .withColumn("month", F.month("tpep_pickup_datetime"))
    )


def prepare_weather(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "weather_hour",
        F.date_trunc("hour", F.col("observation_timestamp"))
    )

    columns = ["weather_hour", "temp", "rhum", "prcp", "coco"]

    if "humidity" in df.columns:
        columns.append("humidity")

    return df.select(*columns)


def prepare_pickup_zones(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough")
    )


def prepare_dropoff_zones(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("location_id").alias("do_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough")
    )


def prepare_air_quality(df: DataFrame) -> DataFrame:
    nyc_counties = ["New York", "Kings", "Queens", "Bronx", "Richmond"]

    df = (
        df
        .filter(
            (F.col("state_name") == "New York")
            & F.col("county_name").isin(nyc_counties)
        )
        .withColumn(
            "air_quality_hour",
            F.date_trunc("hour", F.col("observation_timestamp"))
        )
    )

    aggregations = [
        F.avg("sample_measurement").alias("air_quality_measurement")
    ]

    if "aqi" in df.columns:
        aggregations.append(
            F.avg("aqi").alias("air_quality_aqi")
        )

    return df.groupBy("air_quality_hour").agg(*aggregations)


# ============================================================
# FILTER AFFECTED MONTHS
# ============================================================

def filter_taxi_partitions(df: DataFrame, partitions: set) -> DataFrame:
    condition = " OR ".join(
        f"(year = {year} AND month = {month})"
        for year, month in partitions
    )

    return df.filter(condition)


def filter_timestamp_partitions(
    df: DataFrame,
    timestamp_column: str,
    partitions: set
) -> DataFrame:

    condition = " OR ".join(
        f"(year({timestamp_column}) = {year} "
        f"AND month({timestamp_column}) = {month})"
        for year, month in partitions
    )

    return df.filter(condition)


# ============================================================
# BUILD DATAFRAME
# ============================================================

def build_dataframe(
    spark: SparkSession,
    affected_partitions: set | None = None
) -> DataFrame:

    taxi_df = spark.read.format("delta").load("storage/silver/taxi_trips")
    weather_df = spark.read.format("delta").load("storage/silver/weather")
    air_quality_df = spark.read.format("delta").load("storage/silver/air_quality")
    zones_df = spark.read.format("delta").load("storage/silver/taxi_zones")

    # Incremental refresh: only read affected months
    if affected_partitions:
        taxi_df = filter_taxi_partitions(
            taxi_df,
            affected_partitions
        )

        weather_df = filter_timestamp_partitions(
            weather_df,
            "observation_timestamp",
            affected_partitions
        )

        air_quality_df = filter_timestamp_partitions(
            air_quality_df,
            "observation_timestamp",
            affected_partitions
        )

    taxi_df = prepare_taxi(taxi_df)
    weather_df = prepare_weather(weather_df)
    air_quality_df = prepare_air_quality(air_quality_df)

    pickup_zones = prepare_pickup_zones(zones_df)
    dropoff_zones = prepare_dropoff_zones(zones_df)

    df = taxi_df.join(
        F.broadcast(dropoff_zones),
        on="do_location_id",
        how="left"
    )

    df = df.join(
        F.broadcast(pickup_zones),
        on="pu_location_id",
        how="left"
    )

    df = (
        df
        .join(
            weather_df,
            df["pickup_hour"] == weather_df["weather_hour"],
            "left"
        )
        .drop("weather_hour")
    )

    df = (
        df
        .join(
            air_quality_df,
            df["pickup_hour"] == air_quality_df["air_quality_hour"],
            "left"
        )
        .drop("air_quality_hour")
    )

    return df


# ============================================================
# WRITE GOLD
# ============================================================

def write_full_gold(df: DataFrame):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("year", "month")
        .save(GOLD_PATH)
    )


def write_incremental_gold(
    df: DataFrame,
    partitions: set
):
    predicate = " OR ".join(
        f"(year = {year} AND month = {month})"
        for year, month in partitions
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", predicate)
        .option("mergeSchema", "true")
        .save(GOLD_PATH)
    )


# ============================================================
# MAIN FUNCTION
# ============================================================

def build_integrated_taxi_trips(
    spark: SparkSession,
    affected_partitions: set,
    full_refresh: bool = False
):
    print("\n--- Integrating integrated_taxi_trips ---")
    start = time.perf_counter()

    # First execution or taxi_zones changed
    if full_refresh or not DeltaTable.isDeltaTable(spark, GOLD_PATH):
        print("Full Gold refresh")

        df = build_dataframe(spark)
        write_full_gold(df)

    else:
        if not affected_partitions:
            print("No changes affecting Gold.")
            return

        print(f"Affected partitions: {sorted(affected_partitions)}")

        df = build_dataframe(
            spark,
            affected_partitions
        )

        write_incremental_gold(
            df,
            affected_partitions
        )

    execution_time = time.perf_counter() - start

    print(
        f"Finished integrated_taxi_trips build\n"
        f"Time: {execution_time:.2f}s"
    )