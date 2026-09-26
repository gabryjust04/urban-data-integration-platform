from pathlib import Path

from src.common.config import load_yaml
from src.common.spark import create_spark_session
from src.integration import build_integrated_taxi_trips
from src.ingestion import ingest_data
from src.analytics.data_products import build_data

def main():
    spark = create_spark_session()

    try:
        configs_folder = Path("configs")
        affected_partitions = set()
        full_gold_refresh = False
        changed_datasets = set()
        for config_file in configs_folder.glob("*.yaml"):
            config_content = load_yaml(config_file)
            datasets = config_content.get("datasets", {})

            for dataset_name, dataset_cfg in datasets.items():
                partitions, full_refresh = ingest_data(
                    spark,
                    dataset_name,
                    dataset_cfg
                )

                affected_partitions.update(partitions)
                full_gold_refresh = full_gold_refresh or full_refresh
                if partitions or full_refresh:
                    changed_datasets.add(dataset_name)

        build_integrated_taxi_trips(spark,affected_partitions,full_gold_refresh)

        build_data(spark,affected_partitions,changed_datasets)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()