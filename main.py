from pathlib import Path

from src.common.config import load_yaml
from src.common.spark import create_spark_session
from src.integration import build_integrated_taxi_trips
from src.ingestion import ingest_data


def main():
    spark = create_spark_session()

    try:
        configs_folder = Path("configs")

        for config_file in configs_folder.glob("*.yaml"):
            config_content = load_yaml(config_file)

            datasets = config_content.get("datasets", {})

            for dataset_name, dataset_cfg in datasets.items():
                ingest_data(
                    spark,
                    dataset_name,
                    dataset_cfg,
                )

        build_integrated_taxi_trips(spark)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()