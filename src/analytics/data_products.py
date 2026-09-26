import time
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp, lit


DAILY_PATH = "storage/gold/data_products/daily_mobility_summary"

def add_metadata(df):
    return (
        df
        .withColumn("data_source", lit("integrated_taxi_trips"))
        .withColumn("created_at", current_timestamp())
        .withColumn("refresh_time", current_timestamp())
        .withColumn("schema_version", lit("1.0"))
    )


def build_daily_mobility_summary(spark, affected_partitions=None):
    start = time.perf_counter()

    df = spark.read.format("delta").load("storage/gold/integrated_taxi_trips")

    # Read only affected partitions during incremental refresh
    if affected_partitions:
        condition = " OR ".join(f"(year = {year} AND month = {month})" for year, month in affected_partitions)
        df = df.filter(condition)

    df.createOrReplaceTempView("trips")

    result = spark.sql("""
        SELECT COUNT(*) AS total_trips,
               date_trunc('day', tpep_pickup_datetime) AS day,
               ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
               ROUND(AVG(fare_amount), 2) AS avg_fare,
               ROUND(AVG(passenger_count), 2) AS avg_passengers
        FROM trips
        GROUP BY date_trunc('day', tpep_pickup_datetime)
    """)

    result = result.withColumn("year", F.year("day")).withColumn("month", F.month("day"))
    result = add_metadata(result)

    # First build
    if not DeltaTable.isDeltaTable(spark, DAILY_PATH):
        result.write.format("delta").mode("overwrite").partitionBy("year", "month").save(DAILY_PATH)

    # Incremental refresh
    else:
        predicate = " OR ".join(f"(year = {year} AND month = {month})" for year, month in affected_partitions)
        result.write.format("delta").mode("overwrite").option("replaceWhere", predicate).save(DAILY_PATH)

    print(f"Daily Mobility Summary completed in {time.perf_counter() - start:.2f}s")


def build_taxi_zone_statistics(spark):
    df = spark.read.format("delta").load("storage/gold/integrated_taxi_trips")
    df.printSchema()
    df.createOrReplaceTempView("trips")

    start = time.perf_counter()

    df = spark.sql("""
        SELECT COUNT(*) AS total_trips, pickup_zone,
               ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
               ROUND(AVG(fare_amount), 2) AS avg_fare,
               ROUND(AVG(passenger_count), 2) AS avg_passengers
        FROM trips t
        GROUP BY pickup_zone
    """)

    df = add_metadata(df)
    df.write.format("delta").mode("overwrite").save(
        "storage/gold/data_products/taxi_zone_statistics"
    )

    print(f"Taxi Zone Statistics completed in {time.perf_counter() - start:.2f}s")


def build_weather_impact_summary(spark):
    df = spark.read.format("delta").load("storage/gold/integrated_taxi_trips")
    df.printSchema()
    df.createOrReplaceTempView("trips")

    start = time.perf_counter()

    df = spark.sql("""
        SELECT COUNT(*) AS total_trips, coco,
               ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
               ROUND(AVG(fare_amount), 2) AS avg_fare,
               ROUND(AVG(passenger_count), 2) AS avg_passengers,
               ROUND(AVG(temp), 2) AS avg_temperature,
               ROUND(AVG(rhum), 2) AS avg_humidity,
               ROUND(AVG(prcp), 2) AS avg_precipitation
        FROM trips t
        WHERE coco IS NOT NULL
        GROUP BY coco
    """)

    df = add_metadata(df)
    df.write.format("delta").mode("overwrite").save(
        "storage/gold/data_products/weather_impact_summary"
    )

    print(f"Weather Impact Summary completed in {time.perf_counter() - start:.2f}s")


def build_air_quality_impact_summary(spark):
    df = spark.read.format("delta").load("storage/gold/integrated_taxi_trips")
    df.printSchema()
    df.createOrReplaceTempView("trips")

    start = time.perf_counter()

    df = spark.sql("""
        WITH categorized AS (
            SELECT *,
                CASE
                    WHEN air_quality_measurement < 10 THEN 'Good'
                    WHEN air_quality_measurement < 25 THEN 'Moderate'
                    ELSE 'Poor'
                END AS air_quality_category
            FROM trips t
            WHERE air_quality_measurement IS NOT NULL
        )
        SELECT air_quality_category, COUNT(*) AS total_trips,
               ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
               ROUND(AVG(fare_amount), 2) AS avg_fare,
               ROUND(AVG(passenger_count), 2) AS avg_passengers
        FROM categorized
        GROUP BY air_quality_category
    """)

    df = add_metadata(df)
    df.write.format("delta").mode("overwrite").save(
        "storage/gold/data_products/quality_impact_summary"
    )

    print(f"Air Quality Impact Summary completed in {time.perf_counter() - start:.2f}s")


def build_borough_mobility_summary(spark):
    df = spark.read.format("delta").load("storage/gold/integrated_taxi_trips")
    df.printSchema()
    df.createOrReplaceTempView("trips")

    start = time.perf_counter()

    df = spark.sql("""
        SELECT COUNT(*) AS total_trips, pickup_borough,
               ROUND(AVG(trip_distance), 2) AS avg_trip_distance,
               ROUND(AVG(fare_amount), 2) AS avg_fare,
               ROUND(AVG(passenger_count), 2) AS avg_passengers
        FROM trips t
        GROUP BY pickup_borough
    """)

    df = add_metadata(df)
    df.write.format("delta").mode("overwrite").save(
        "storage/gold/data_products/borough_mobility_summary"
    )

    print(f"Borough Mobility Summary completed in {time.perf_counter() - start:.2f}s")


def build_data(spark,affected_partitions,changed_datasets):
    if "taxi_trips" in changed_datasets:
        build_daily_mobility_summary(spark,affected_partitions)
    build_taxi_zone_statistics(spark)
    build_weather_impact_summary(spark)
    build_air_quality_impact_summary(spark)
    build_borough_mobility_summary(spark)


# Simple examples of reusable data product queries
def use_new_data(spark):
    df = spark.read.format("delta").load(
        "storage/gold/data_products/daily_mobility_summary"
    )
    df.createOrReplaceTempView("daily_mobility_summary")

    start = time.perf_counter()

    spark.sql("""
        SELECT day, avg_fare
        FROM daily_mobility_summary
        ORDER BY avg_fare DESC
        LIMIT 10
    """).show()

    print(f"Daily Mobility query completed in {time.perf_counter() - start:.2f}s")

    df = spark.read.format("delta").load(
        "storage/gold/data_products/weather_impact_summary"
    )
    df.createOrReplaceTempView("weather_impact_summary")

    start = time.perf_counter()

    spark.sql("""
        SELECT coco, total_trips, avg_trip_distance, avg_fare
        FROM weather_impact_summary
        ORDER BY total_trips DESC
    """).show()

    print(f"Weather Impact query completed in {time.perf_counter() - start:.2f}s")