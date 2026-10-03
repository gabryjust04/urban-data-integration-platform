from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from src.common.spark import create_spark_session
from src.ml.dataset import build_ml_dataset


MODEL_PATH = "storage/models/taxi_demand"


def predict_demand(spark, model, data):
    df = spark.createDataFrame([data])
    return model.transform(df).select("prediction").first()["prediction"]


def show_historical_context(ml_dataset, zone, hour):
    historical = (
        ml_dataset
        .filter((F.col("pickup_zone") == zone) & (F.col("hour_of_day") == hour))
        .agg(
            F.count("*").alias("samples"),
            F.avg("taxi_demand").alias("average"),
            F.min("taxi_demand").alias("minimum"),
            F.max("taxi_demand").alias("maximum")
        )
        .first()
    )

    print(
        f"Historical demand: avg={historical['average']:.2f}, "
        f"min={historical['minimum']}, max={historical['maximum']}, "
        f"samples={historical['samples']}"
    )


def main():
    spark = create_spark_session()

    try:
        model = PipelineModel.load(MODEL_PATH)
        ml_dataset = build_ml_dataset(spark).cache()

        examples = [
            {
                "pickup_zone": "Midtown Center",
                "pickup_borough": "Manhattan",
                "hour_of_day": 18,
                "day_of_week": 6,
                "month": 6,
                "temp": 22.0,
                "rhum": 65.0,
                "prcp": 0.0,
                "air_quality_measurement": 8.0
            },
            {
                "pickup_zone": "Upper East Side North",
                "pickup_borough": "Manhattan",
                "hour_of_day": 18,
                "day_of_week": 3,
                "month": 6,
                "temp": 20.0,
                "rhum": 60.0,
                "prcp": 0.0,
                "air_quality_measurement": 6.0
            },
            {
                "pickup_zone": "Brooklyn Heights",
                "pickup_borough": "Brooklyn",
                "hour_of_day": 9,
                "day_of_week": 2,
                "month": 6,
                "temp": 18.0,
                "rhum": 70.0,
                "prcp": 0.0,
                "air_quality_measurement": 7.0
            }
        ]

        for i, data in enumerate(examples, start=1):
            prediction = predict_demand(spark, model, data)

            print(f"\n========== PREDICTION {i} ==========")
            print(f"Zone: {data['pickup_zone']}")
            print(f"Hour: {data['hour_of_day']}:00")
            print(f"Predicted taxi demand: {prediction:.2f}")

            show_historical_context(
                ml_dataset,
                data["pickup_zone"],
                data["hour_of_day"]
            )

        ml_dataset.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()