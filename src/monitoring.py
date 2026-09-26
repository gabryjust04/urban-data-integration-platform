from delta.tables import DeltaTable

from src.common.spark import create_spark_session


METADATA_PATH = "storage/metadata"


def run_monitoring_queries(spark):
    if not DeltaTable.isDeltaTable(spark, METADATA_PATH):
        print("No monitoring metadata found.")
        return

    metadata = spark.read.format("delta").load(METADATA_PATH)
    metadata.createOrReplaceTempView("pipeline_metadata")

    print("\n--- Dataset with most validation failures ---")
    spark.sql("""
        SELECT dataset_name,
               SUM(validation_failures) AS total_validation_failures
        FROM pipeline_metadata
        GROUP BY dataset_name
        ORDER BY total_validation_failures DESC
    """).show()

    print("\n--- Dataset with longest average processing time ---")
    spark.sql("""
        SELECT dataset_name,
               ROUND(AVG(execution_time_seconds), 2) AS avg_execution_time_seconds
        FROM pipeline_metadata
        WHERE status = 'success'
        GROUP BY dataset_name
        ORDER BY avg_execution_time_seconds DESC
    """).show()

    print("\n--- Rejected records per execution ---")
    spark.sql("""
        SELECT ingestion_timestamp,
               dataset_name,
               processed_records,
               rejected_records
        FROM pipeline_metadata
        ORDER BY ingestion_timestamp
    """).show(truncate=False)

    print("\n--- Processing time trend ---")
    spark.sql("""
        SELECT ingestion_timestamp,
               dataset_name,
               execution_time_seconds
        FROM pipeline_metadata
        ORDER BY dataset_name, ingestion_timestamp
    """).show(truncate=False)

    print("\n--- Pipeline execution summary ---")
    spark.sql("""
        SELECT ingestion_timestamp,
               dataset_name,
               schema_version,
               processed_records,
               inserted_records,
               duplicate_records,
               rejected_records,
               validation_failures,
               ROUND(execution_time_seconds, 2) AS execution_time_seconds,
               status
        FROM pipeline_metadata
        ORDER BY ingestion_timestamp
    """).show(truncate=False)

    print("\n--- Failed executions ---")
    spark.sql("""
        SELECT ingestion_timestamp,
               dataset_name,
               error_message
        FROM pipeline_metadata
        WHERE status = 'failed'
        ORDER BY ingestion_timestamp
    """).show(truncate=False)


def main():
    spark = create_spark_session()

    try:
        run_monitoring_queries(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()