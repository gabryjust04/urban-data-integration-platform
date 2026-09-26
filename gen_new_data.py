from pyspark.sql import functions as F


def gen_new_taxi_trips(spark):
    df = spark.read.parquet("raw/taxi_trips")

    # 10% new trips
    sample_df = df.sample(withReplacement=False, fraction=0.10, seed=42)

    max_ts = df.agg(F.max("tpep_pickup_datetime")).first()[0]
    min_sample_ts = sample_df.agg(F.min("tpep_pickup_datetime")).first()[0]
    shift_seconds = int((max_ts - min_sample_ts).total_seconds()) + 3600

    new_df = (
        sample_df
        .withColumn(
            "tpep_pickup_datetime",
            F.expr(f"tpep_pickup_datetime + INTERVAL {shift_seconds} SECONDS")
        )
        .withColumn(
            "tpep_dropoff_datetime",
            F.expr(f"tpep_dropoff_datetime + INTERVAL {shift_seconds} SECONDS")
        )
        .withColumn("fare_amount", F.col("fare_amount") * 1.05)
    )

    # 2% exact duplicates from original data
    duplicates_df = df.sample(withReplacement=False, fraction=0.02, seed=43)

    update_df = new_df.unionByName(duplicates_df)

    # One new parquet part file
    update_df.coalesce(1).write.mode("append").parquet("raw/taxi_trips")


def gen_new_weather(spark):
    df = spark.read.option("header", True).option("inferSchema", True).csv("raw/weather")

    df = df.withColumn(
        "_timestamp",
        F.make_timestamp(
            F.col("year"),
            F.col("month"),
            F.col("day"),
            F.col("hour"),
            F.lit(0),
            F.lit(0)
        )
    )

    max_ts = df.agg(F.max("_timestamp")).first()[0]

    # Last 24 hours
    new_df = df.filter(
        F.col("_timestamp") >= F.lit(max_ts) - F.expr("INTERVAL 23 HOURS")
    )

    # Move them to the following 24 hours
    new_df = (
        new_df
        .withColumn("_timestamp", F.col("_timestamp") + F.expr("INTERVAL 24 HOURS"))
        .withColumn("year", F.year("_timestamp"))
        .withColumn("month", F.month("_timestamp"))
        .withColumn("day", F.dayofmonth("_timestamp"))
        .withColumn("hour", F.hour("_timestamp"))
        .withColumn("humidity", (F.rand(seed=42) * 80 + 20).cast("double"))
        .drop("_timestamp")
    )

    new_df.coalesce(1).write.mode("append").option("header", True).csv("raw/weather")


def gen_new_air_quality(spark):
    df = spark.read.option("header", True).csv("raw/air_quality")

    max_date = df.agg(F.max("Date Local")).first()[0]

    new_df = df.filter(F.col("Date Local") == max_date)

    new_df = (
        new_df
        .withColumn(
            "Date Local",
            F.date_format(
                F.date_add(F.to_date("Date Local", "yyyy-MM-dd"), 1),
                "yyyy-MM-dd"
            )
        )
        .withColumn(
            "Date GMT",
            F.date_format(
                F.date_add(F.to_date("Date GMT", "yyyy-MM-dd"), 1),
                "yyyy-MM-dd"
            )
        )
        .withColumn(
            "aqi",
            (F.rand(seed=42) * 151).cast("int")
        )
    )

    new_df.coalesce(1) \
        .write \
        .mode("append") \
        .option("header", True) \
        .csv("raw/air_quality")


def gen_new_data(spark):
    gen_new_taxi_trips(spark)
    gen_new_weather(spark)
    gen_new_air_quality(spark)