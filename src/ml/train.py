import time

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import Imputer, OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import functions as F

from src.ml.dataset import build_ml_dataset


MODEL_PATH = "storage/models/taxi_demand"


def build_feature_pipeline(config):
    numerical = config["numerical_features"]
    categorical = config["categorical_features"]

    imputed = [f"{col}_imputed" for col in numerical]
    indexed = [f"{col}_index" for col in categorical]
    encoded = [f"{col}_encoded" for col in categorical]

    imputer = Imputer(inputCols=numerical, outputCols=imputed, strategy="median")

    indexers = [
        StringIndexer(inputCol=col, outputCol=f"{col}_index", handleInvalid="keep")
        for col in categorical
    ]

    encoder = OneHotEncoder(inputCols=indexed, outputCols=encoded, handleInvalid="keep")
    assembler = VectorAssembler(inputCols=imputed + encoded, outputCol="features")

    return Pipeline(stages=[imputer, *indexers, encoder, assembler])


def split_dataset(df):
    train = df.filter(F.col("pickup_hour") < "2024-05-01")
    validation = df.filter((F.col("pickup_hour") >= "2024-05-01") & (F.col("pickup_hour") < "2024-06-01"))
    test = df.filter(F.col("pickup_hour") >= "2024-06-01")

    return train, validation, test


def evaluate(predictions, target):
    evaluator = RegressionEvaluator(labelCol=target, predictionCol="prediction")

    rmse = evaluator.setMetricName("rmse").evaluate(predictions)
    mae = evaluator.setMetricName("mae").evaluate(predictions)
    r2 = evaluator.setMetricName("r2").evaluate(predictions)

    return rmse, mae, r2


def train(spark, config):
    total_start = time.perf_counter()

    start = time.perf_counter()
    ml_dataset = build_ml_dataset(spark).cache()
    dataset_rows = ml_dataset.count()
    dataset_time = time.perf_counter() - start

    start = time.perf_counter()
    train_df, validation_df, test_df = split_dataset(ml_dataset)
    train_rows = train_df.count()
    validation_rows = validation_df.count()
    test_rows = test_df.count()
    split_time = time.perf_counter() - start

    print(f"\nML dataset rows: {dataset_rows}")
    print(f"Training rows: {train_rows}")
    print(f"Validation rows: {validation_rows}")
    print(f"Test rows: {test_rows}")

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

    start = time.perf_counter()
    validation_predictions = model.transform(validation_df)
    val_rmse, val_mae, val_r2 = evaluate(validation_predictions, config["target"])
    validation_time = time.perf_counter() - start

    start = time.perf_counter()
    test_predictions = model.transform(test_df)
    test_rmse, test_mae, test_r2 = evaluate(test_predictions, config["target"])
    test_time = time.perf_counter() - start

    print(f"\nValidation — RMSE: {val_rmse:.2f}, MAE: {val_mae:.2f}, R²: {val_r2:.4f}")
    print(f"Test       — RMSE: {test_rmse:.2f}, MAE: {test_mae:.2f}, R²: {test_r2:.4f}")

    start = time.perf_counter()
    model.write().overwrite().save(MODEL_PATH)
    save_time = time.perf_counter() - start

    total_time = time.perf_counter() - total_start

    print("\n================ ML TIMINGS ================")
    print(f"Dataset creation: {dataset_time:.2f}s")
    print(f"Dataset split:    {split_time:.2f}s")
    print(f"Model training:   {training_time:.2f}s")
    print(f"Validation:       {validation_time:.2f}s")
    print(f"Test:             {test_time:.2f}s")
    print(f"Model saving:     {save_time:.2f}s")
    print("--------------------------------------------")
    print(f"Total time:       {total_time:.2f}s")
    print("============================================")

    ml_dataset.unpersist()

    return model