import yaml

from src.common.spark import create_spark_session
from src.ml.train import train


CONFIG_PATH = "src/ml/configs/taxi_demand.yaml"


def main():
    spark = create_spark_session()

    try:
        with open(CONFIG_PATH, "r") as file:
            config = yaml.safe_load(file)

        train(spark, config)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()