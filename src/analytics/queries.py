import time





def run_analytics_script(spark,cache=False,aqe=False):
    df = spark.read.format("delta").load("storage/gold/integrated_taxi_trips")
    df.printSchema()
    df.createOrReplaceTempView("trips")
    if cache:
            print("--- Now using cache ---")
            df.cache()
            df.count()
    else:
         print("--- Not using cache ---")
    if aqe:
        print("--- Now using AQE ---")
        spark.conf.set("spark.sql.adaptive.enabled", "true")
    else:
        spark.conf.set("spark.sql.adaptive.enabled", "false")

    # 1. Monthly taxi demand for each zone
    print("\n--- Monthly taxi demand for each zone ---")
    start = time.perf_counter()
    query = spark.sql("""
        SELECT date_trunc('month', tpep_pickup_datetime) AS trip_month, pickup_zone, COUNT(*) AS monthly_demand
        FROM trips
        GROUP BY pickup_zone, date_trunc('month', tpep_pickup_datetime)
    """)
    query.show()
    execution_time = time.perf_counter() - start
    query.explain()
    print(
        f"Finished Monthly taxi demand for each zone\nTime: {execution_time:.2f}s"
    )

    # 2. Average trip distance under different weather conditions
    print("\n--- Average trip distance under different weather conditions ---")
    start = time.perf_counter()
    query = spark.sql("""
        SELECT 
            coco,
            ROUND(AVG(trip_distance), 2) AS avg_trip_distance
        FROM trips
        GROUP BY coco
    """)
    query.show()
    execution_time = time.perf_counter() - start
    query.explain()
    print(
        f"Finished Average trip distance under different weather conditions\nTime: {execution_time:.2f}s"
    )

    # 3. Relationship between air quality and taxi demand
    print("\n--- Relationship between air quality and taxi demand ---")
    start = time.perf_counter()
    query = spark.sql("""
        WITH hourly_demand AS (
            SELECT 
                COUNT(*) AS trip_count,
                air_quality_measurement,
                date_trunc('hour', tpep_pickup_datetime) AS hour
            FROM trips
            GROUP BY date_trunc('hour', tpep_pickup_datetime), air_quality_measurement
        ),
        categorized AS (
            SELECT *,
                CASE 
                    WHEN air_quality_measurement < 10 THEN 'Good'
                    WHEN air_quality_measurement < 25 THEN 'Moderate'
                    ELSE 'Poor'
                END AS air_quality_category
            FROM hourly_demand
        )
        SELECT 
            air_quality_category,
            FORMAT_NUMBER(AVG(trip_count), 2) AS avg_hourly_demand
        FROM categorized
        GROUP BY air_quality_category
    """)
    query.show()
    execution_time = time.perf_counter() - start
    query.explain()
    print(
        f"Finished Relationship between air quality and taxi demand\nTime: {execution_time:.2f}s"
    )

    # 4. Taxi zones with the largest variation in demand under different weather conditions
    print(
        "\n--- Taxi zones with the largest variation in demand under different"
        " weather conditions ---"
    )
    start = time.perf_counter()
    query = spark.sql("""
        WITH hourly_zone_weather AS (
            SELECT 
                pickup_zone,
                coco,
                date_trunc('hour', tpep_pickup_datetime) AS pickup_hour,
                COUNT(*) AS hourly_trips
            FROM trips
            GROUP BY pickup_zone, coco, date_trunc('hour', tpep_pickup_datetime)
        ),
        zone_weather_stats AS (
            SELECT 
                pickup_zone,
                coco,
                AVG(hourly_trips) AS avg_trips_in_weather,
                COUNT(*) AS hours_observed
            FROM hourly_zone_weather
            GROUP BY pickup_zone, coco
            HAVING COUNT(*) >= 5
        )
        SELECT 
            pickup_zone,
            COUNT(DISTINCT coco) AS distinct_weather_types,
            ROUND(AVG(avg_trips_in_weather), 2) AS baseline_avg_trips,
            ROUND(MIN(avg_trips_in_weather), 2) AS min_weather_avg,
            ROUND(MAX(avg_trips_in_weather), 2) AS max_weather_avg,
            ROUND(STDDEV(avg_trips_in_weather), 2) AS weather_stddev,
            ROUND(STDDEV(avg_trips_in_weather) / AVG(avg_trips_in_weather), 4) AS weather_variation_score
        FROM zone_weather_stats
        GROUP BY pickup_zone
        HAVING COUNT(DISTINCT coco) > 1  
        ORDER BY weather_variation_score DESC NULLS LAST
    """)
    query.show(20, truncate=False)
    execution_time = time.perf_counter() - start
    query.explain()
    print(
        f"Finished Taxi zones with largest variation\nTime: {execution_time:.2f}s"
    )

    # 5. Peak travel hours for each day of the week
    print("\n--- Peak travel hours for each day of the week ---")
    start = time.perf_counter()
    query = spark.sql("""
        WITH hourly_trips AS (
            SELECT 
                dayofweek(tpep_pickup_datetime) AS day,
                hour(tpep_pickup_datetime) AS hour,
                COUNT(*) AS trips_number
            FROM trips
            GROUP BY dayofweek(tpep_pickup_datetime), hour(tpep_pickup_datetime)
        ),
        daily_max AS (
            SELECT day, MAX(trips_number) AS max_trips
            FROM hourly_trips
            GROUP BY day
        )
        SELECT HT.hour AS peak_hour, HT.day
        FROM hourly_trips HT
        JOIN daily_max DM 
          ON HT.day = DM.day AND HT.trips_number = DM.max_trips
        ORDER BY HT.day
    """)
    query.show()
    execution_time = time.perf_counter() - start
    query.explain()
    print(
        f"Finished Peak travel hours for each day of the week\nTime: {execution_time:.2f}s"
    )

    # 6. Monthly trends in taxi demand
    print("\n--- Monthly trends in taxi demand ---")
    start = time.perf_counter()
    query = spark.sql("""
        WITH months_trips AS (
            SELECT 
                date_trunc('month', tpep_pickup_datetime) AS month,
                COUNT(*) AS total_trips
            FROM trips
            GROUP BY date_trunc('month', tpep_pickup_datetime)
        )
        SELECT 
            total_trips,
            month,
            LAG(total_trips, 1) OVER (ORDER BY month) AS prev_month_trips,
            total_trips - LAG(total_trips, 1) OVER (ORDER BY month) AS net_change
        FROM months_trips
        ORDER BY month
    """)
    query.show()
    execution_time = time.perf_counter() - start
    query.explain()
    print(
        f"Finished Monthly trends in taxi demand\nTime: {execution_time:.2f}s"
    )

    if cache:
        df.unpersist()



def run_query_4_with_pruning(spark):
    df = spark.read.format("delta").load("storage/gold/integrated_taxi_trips")
    df.printSchema()
    df.createOrReplaceTempView("trips")

        # 4. Taxi zones with the largest variation in demand under different weather conditions
    print(
        "\n--- Taxi zones with the largest variation in demand under different"
        " weather conditions with partition pruning, only 2024 data---"
    )
    start = time.perf_counter()
    spark.sql("""
        WITH hourly_zone_weather AS (
            SELECT 
                pickup_zone,
                coco,
                date_trunc('hour', tpep_pickup_datetime) AS pickup_hour,
                COUNT(*) AS hourly_trips
            FROM trips
            WHERE year = 2024
            GROUP BY pickup_zone, coco, date_trunc('hour', tpep_pickup_datetime)
        ),
        zone_weather_stats AS (
            SELECT 
                pickup_zone,
                coco,
                AVG(hourly_trips) AS avg_trips_in_weather,
                COUNT(*) AS hours_observed
            FROM hourly_zone_weather
            GROUP BY pickup_zone, coco
            HAVING COUNT(*) >= 5
        )
        SELECT 
            pickup_zone,
            COUNT(DISTINCT coco) AS distinct_weather_types,
            ROUND(AVG(avg_trips_in_weather), 2) AS baseline_avg_trips,
            ROUND(MIN(avg_trips_in_weather), 2) AS min_weather_avg,
            ROUND(MAX(avg_trips_in_weather), 2) AS max_weather_avg,
            ROUND(STDDEV(avg_trips_in_weather), 2) AS weather_stddev,
            ROUND(STDDEV(avg_trips_in_weather) / AVG(avg_trips_in_weather), 4) AS weather_variation_score
        FROM zone_weather_stats
        GROUP BY pickup_zone
        HAVING COUNT(DISTINCT coco) > 1  
        ORDER BY weather_variation_score DESC NULLS LAST
    """).show(20, truncate=False)
    execution_time = time.perf_counter() - start
    print(
        f"Finished Taxi zones with largest variation with pruning\nTime: {execution_time:.2f}s"
    )


def run_query_join_with_and_without_broadcast(spark):
    taxi_trips_df = spark.read.format("delta").load("storage/silver/taxi_trips")
    weather_df = spark.read.format("delta").load("storage/silver/weather")
    taxi_zones_df = spark.read.format("delta").load("storage/silver/taxi_zones")
    taxi_trips_df.createOrReplaceTempView("trips")
    weather_df.createOrReplaceTempView("weather")
    taxi_zones_df.createOrReplaceTempView("zones")

    # Forcing disable auto-broadcasting to simulate standard shuffle join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    start = time.perf_counter()
    unoptimized_join = spark.sql("""SELECT COUNT(*) as total_trips, zone,coco 
    FROM trips t
    JOIN zones z ON t.pu_location_id = z.location_id
    JOIN weather w ON date_trunc('hour', t.tpep_pickup_datetime) = w.observation_timestamp
    GROUP BY z.zone,w.coco
    """)
    unoptimized_join.show(10)
    print(
        f"Finished with Shuffle Join in: {time.perf_counter() - start:.2f}s\n"
    )

    print("--- Unoptimized Plan (Look for SortMergeJoin / Exchange) ---")
    unoptimized_join.explain()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
    start = time.perf_counter()
    optimized_join = spark.sql("""SELECT /*+ BROADCAST(z), BROADCAST(w) */ COUNT(*) as total_trips, zone,coco 
    FROM trips t
    JOIN zones z ON t.pu_location_id = z.location_id
    JOIN weather w ON date_trunc('hour', t.tpep_pickup_datetime) = w.observation_timestamp
    GROUP BY z.zone,w.coco
    """)
    optimized_join.show(10)
    print(
        f"Finished with Broadcast Join in: {time.perf_counter() - start:.2f}s\n"
    )


    print("\n--- Optimized Plan (Look for BroadcastHashJoin / BroadcastExchange) ---")
    optimized_join.explain()
    

def build_daily_mobility_summary(spark):
    result = spark.sql("""

    """)
     

def build_data(spark):
    build_daily_mobility_summary(spark)


def run_analytics(spark):
    run_analytics_script(spark,cache=False)
    run_analytics_script(spark,cache=True)
    run_analytics_script(spark,aqe=True)
    run_analytics_script(spark,aqe=True,cache=True)
    run_query_4_with_pruning(spark)
    run_query_join_with_and_without_broadcast(spark)
    build_data(spark)



