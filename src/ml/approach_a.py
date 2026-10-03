import time
import yaml

from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import functions as F

from src.common.spark import create_spark_session
from src.ml.train import build_feature_pipeline, split_dataset, evaluate



CONFIG_PATH = "src/ml/configs/taxi_demand.yaml"


def build_dataset_from_raw(spark):
    # Taxi trips
    taxi = (
        spark.read.parquet("raw/taxi_trips")
        .filter(F.col("tpep_pickup_datetime").isNotNull())
        .filter(F.col("tpep_dropoff_datetime").isNotNull())
        .filter(F.col("trip_distance") >= 0)
        .withColumn("pickup_hour", F.date_trunc("hour", "tpep_pickup_datetime"))
        .withColumnRenamed("PULocationID", "pu_location_id")
        .select("pickup_hour", "pu_location_id")
    )

    # Taxi zones
    zones = (
        spark.read.option("header", True).option("inferSchema", True).csv("raw/taxi_zones")
        .select(
            F.col("LocationID").cast("int").alias("pu_location_id"),
            F.col("Zone").alias("pickup_zone"),
            F.col("Borough").alias("pickup_borough")
        )
        .dropDuplicates(["pu_location_id"])
    )

    # Weather
    weather = (
    spark.read.option("header", True).csv("raw/weather")
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("month", F.col("month").cast("int"))
    .withColumn("day", F.col("day").cast("int"))
    .withColumn("hour", F.col("hour").cast("int"))
    .withColumn("temp", F.col("temp").cast("double"))
    .withColumn("rhum", F.col("rhum").cast("double"))
    .withColumn("prcp", F.col("prcp").cast("double"))
    .filter((F.col("rhum") >= 0) & (F.col("rhum") <= 100))
    .filter(F.col("prcp") >= 0)
    .withColumn(
        "weather_hour",
        F.to_timestamp(
            F.concat_ws(
                " ",
                F.concat_ws("-", "year", "month", "day"),
                F.concat(F.col("hour"), F.lit(":00"))
            ),
            "yyyy-M-d H:mm"
        )
    )
    .select("weather_hour", "temp", "rhum", "prcp")
    .dropDuplicates(["weather_hour"])
)

    # Air quality
    air = spark.read.option("header", True).option("inferSchema", True).csv("raw/air_quality")
    nyc_counties = ["New York", "Kings", "Queens", "Bronx", "Richmond"]

    air = (
        air
        .filter((F.col("State Name") == "New York") & F.col("County Name").isin(nyc_counties))
        .withColumn(
            "air_quality_hour",
            F.date_trunc(
                "hour",
                F.to_timestamp(
                    F.concat_ws(" ", F.col("Date Local"), F.col("Time Local")),
                    "yyyy-MM-dd HH:mm"
                )
            )
        )
        .filter(F.col("air_quality_hour").isNotNull())
        .groupBy("air_quality_hour")
        .agg(F.avg(F.col("Sample Measurement").cast("double")).alias("air_quality_measurement"))
    )

    # Build hourly taxi demand
    dataset = (
        taxi
        .join(F.broadcast(zones), "pu_location_id", "left")
        .groupBy("pickup_hour", "pu_location_id", "pickup_zone", "pickup_borough")
        .agg(F.count("*").alias("taxi_demand"))
        .join(weather, F.col("pickup_hour") == F.col("weather_hour"), "left")
        .drop("weather_hour")
        .join(air, F.col("pickup_hour") == F.col("air_quality_hour"), "left")
        .drop("air_quality_hour")
        .withColumn("hour_of_day", F.hour("pickup_hour"))
        .withColumn("day_of_week", F.dayofweek("pickup_hour"))
        .withColumn("month", F.month("pickup_hour"))
    )

    return dataset


def main():
    spark = create_spark_session()

    try:
        with open(CONFIG_PATH, "r") as file:
            config = yaml.safe_load(file)

        total_start = time.perf_counter()

        # Raw preprocessing + integration
        start = time.perf_counter()
        dataset = build_dataset_from_raw(spark).cache()
        rows = dataset.count()
        preparation_time = time.perf_counter() - start

        train_df, validation_df, test_df = split_dataset(dataset)

        # Same feature engineering pipeline and model as Approach B
        feature_pipeline = build_feature_pipeline(config)

        regressor = RandomForestRegressor(
            featuresCol="features",
            labelCol=config["target"],
            numTrees=50,
            maxDepth=8,
            seed=42
        )

        pipeline = Pipeline(stages=feature_pipeline.getStages() + [regressor])

        start = time.perf_counter()
        model = pipeline.fit(train_df)
        training_time = time.perf_counter() - start

        validation_predictions = model.transform(validation_df)
        test_predictions = model.transform(test_df)

        val_rmse, val_mae, val_r2 = evaluate(validation_predictions, config["target"])
        test_rmse, test_mae, test_r2 = evaluate(test_predictions, config["target"])

        total_time = time.perf_counter() - total_start

        print("\n================ APPROACH A ================")
        print(f"Dataset rows:       {rows}")
        print(f"Raw preprocessing:  {preparation_time:.2f}s")
        print(f"Model training:     {training_time:.2f}s")
        print(f"Total time:         {total_time:.2f}s")
        print("--------------------------------------------")
        print(f"Validation RMSE: {val_rmse:.2f}, MAE: {val_mae:.2f}, R²: {val_r2:.4f}")
        print(f"Test RMSE:       {test_rmse:.2f}, MAE: {test_mae:.2f}, R²: {test_r2:.4f}")
        print("============================================")

        dataset.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()