import time
import pyspark.sql.functions as F


def build_ml_dataset(spark):
    start = time.perf_counter()

    df = spark.read.format("delta").load("storage/gold/integrated_taxi_trips")

    ml_dataset = (
        df.groupBy(
            "pickup_hour",
            "pu_location_id",
            "pickup_zone",
            "pickup_borough"
        )
        .agg(
            F.count("*").alias("taxi_demand"),
            F.first("temp", ignorenulls=True).alias("temp"),
            F.first("rhum", ignorenulls=True).alias("rhum"),
            F.first("prcp", ignorenulls=True).alias("prcp"),
            F.first("coco", ignorenulls=True).alias("coco"),
            F.first("air_quality_measurement", ignorenulls=True).alias("air_quality_measurement"),
            F.first("air_quality_aqi", ignorenulls=True).alias("air_quality_aqi")
        )
        .withColumn("hour_of_day", F.hour("pickup_hour"))
        .withColumn("day_of_week", F.dayofweek("pickup_hour"))
        .withColumn("month", F.month("pickup_hour"))
    )

    execution_time = time.perf_counter() - start
    print(f"ML dataset built in {execution_time:.2f}s")

    return ml_dataset