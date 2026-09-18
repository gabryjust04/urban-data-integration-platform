from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("urban-data-platform")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()