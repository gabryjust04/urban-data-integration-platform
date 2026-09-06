from pathlib import Path

import yaml
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.ingestion import ingest_data


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("urban-data-integration-platform")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config(
            "spark.sql.ansi.enabled",
            "false"
        )
    )

    return configure_spark_with_delta_pip(
        builder
    ).getOrCreate()


def load_yaml(file_path: Path) -> dict:
    with open(file_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    spark = create_spark_session()

    configs_folder = Path("configs")

    try:
        for config_file in configs_folder.glob("*.yaml"):

            config_content = load_yaml(config_file)

            datasets = config_content.get(
                "datasets", {}
            )

            for dataset_name, dataset_cfg in datasets.items():
                ingest_data(
                    spark,
                    dataset_name,
                    dataset_cfg
                )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()