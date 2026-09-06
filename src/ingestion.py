import re
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ============================================================
# READ DATASET
# ============================================================

def read_dataset(spark: SparkSession, config: dict) -> DataFrame:
    fmt = config["format"].lower()
    path = config["input_path"]

    if fmt == "csv":
        return (
            spark.read
            .option("header", config.get("header", True))
            .option("delimiter", config.get("delimiter", ","))
            .csv(path)
        )

    if fmt == "parquet":
        return spark.read.parquet(path)

    raise ValueError(f"Unsupported format: {fmt}")


# ============================================================
# SCHEMA VALIDATION + COLUMN RENAMING
# ============================================================

def validate_and_rename(df: DataFrame, config: dict) -> DataFrame:
    columns_cfg = config.get("columns", {})

    expected = set(columns_cfg.keys())
    actual = set(df.columns)

    missing = expected - actual

    extra = actual-expected

    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if extra:
            raise ValueError(f"Extra columns: {extra}")

    snake_case_pattern = r"^[a-z][a-z0-9_]*$"

    for source_name, meta in columns_cfg.items():
        target_name = meta.get("target", source_name)

        if not re.fullmatch(snake_case_pattern, target_name):
            raise ValueError(f"Column '{target_name}' is not snake_case")

        if source_name != target_name:
            df = df.withColumnRenamed(
                source_name,
                target_name
            )

    return df


# ============================================================
# BRONZE
# ============================================================

def write_to_bronze(df: DataFrame, config: dict):
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(config["bronze_path"])
    )

    print(f"-> Bronze saved in {config['bronze_path']}")


# ============================================================
# CLEAN STRINGS
# ============================================================

def clean_strings(df: DataFrame) -> DataFrame:
    null_values = ["", "NA", "N/A", "NULL"]

    for field in df.schema.fields:

        if isinstance(field.dataType, StringType):
            cleaned = F.trim(F.col(field.name))

            df = df.withColumn(
                field.name,
                F.when(
                    cleaned.isin(*null_values),
                    F.lit(None)
                ).otherwise(cleaned)
            )

    return df


# ============================================================
# TYPE CASTING
# ============================================================

def cast_types(df: DataFrame, config: dict) -> DataFrame:
    columns_cfg = config.get("columns", {})

    for source_name, meta in columns_cfg.items():
        target_name = meta.get("target", source_name)
        target_type = meta.get("type")

        if not target_type:
            continue

        original = F.col(target_name)
        casted = original.cast(target_type)

        # If the original value exists but cannot be converted,
        # the row becomes invalid.
        invalid_cast = (original.isNotNull() & casted.isNull() )

        df = df.withColumn(
            "_is_valid",
            F.col("_is_valid") & ~invalid_cast
        )

        df = df.withColumn(
            target_name,
            casted
        )

    return df


# ============================================================
# TIMESTAMP CREATION
# ============================================================

def build_timestamps(df: DataFrame, config: dict) -> DataFrame:
    timestamp_cfg = config.get(
        "timestamp_columns", {}
    )

    for target_col, meta in timestamp_cfg.items():

        mode = meta["mode"]
        cols = meta["source_columns"]

        # Weather:
        # year + month + day + hour
        if mode == "components":

            timestamp = F.make_timestamp(
                F.col(cols[0]),
                F.col(cols[1]),
                F.col(cols[2]),
                F.col(cols[3]),
                F.lit(0),
                F.lit(0)
            )

        # Air Quality:
        # date_local + time_local
        elif mode == "concat":

            value = F.concat_ws(
                " ",
                *[F.col(c) for c in cols]
            )

            timestamp = F.to_timestamp(
                value,
                meta["format"]
            )

        else:
            raise ValueError(
                f"Unknown timestamp mode: {mode}"
            )

        df = df.withColumn(
            target_col,
            timestamp
        )

    return df


# ============================================================
# DATA QUALITY
# ============================================================

def apply_quality_rules(
    df: DataFrame,
    config: dict
) -> DataFrame:

    quality = config.get("quality_rules", {})

    # Primary key columns cannot be NULL
    for col_name in config.get("primary_key", []):

        df = df.withColumn(
            "_is_valid",
            F.col("_is_valid")
            & F.col(col_name).isNotNull()
        )

    # Other required columns
    for col_name in quality.get("not_null", []):

        df = df.withColumn(
            "_is_valid",
            F.col("_is_valid")
            & F.col(col_name).isNotNull()
        )

    # Required timestamps must be valid
    for col_name in quality.get(
        "required_timestamps", []
    ):

        df = df.withColumn(
            "_is_valid",
            F.col("_is_valid")
            & F.col(col_name).isNotNull()
        )

    # Numeric ranges
    for col_name, bounds in quality.get(
        "numeric_ranges", {}
    ).items():

        if "min" in bounds:

            valid_min = (
                F.col(col_name).isNull()
                | (F.col(col_name) >= bounds["min"])
            )

            df = df.withColumn(
                "_is_valid",
                F.col("_is_valid") & valid_min
            )

        if "max" in bounds:

            valid_max = (
                F.col(col_name).isNull()
                | (F.col(col_name) <= bounds["max"])
            )

            df = df.withColumn(
                "_is_valid",
                F.col("_is_valid") & valid_max
            )

    return df


# ============================================================
# DUPLICATES
# ============================================================

def deduplicate_records(
    df: DataFrame,
    primary_key: list
) -> DataFrame:

    if primary_key:
        return df.dropDuplicates(primary_key)

    # Taxi Trips has no natural primary key,
    # so only exact duplicate rows are removed.
    return df.dropDuplicates()


# ============================================================
# SILVER
# ============================================================

def write_to_silver(
    df: DataFrame,
    config: dict
):

    partition_columns = config.get(
        "partition_columns", []
    )

    partition_source = config.get(
        "partition_source_timestamp"
    )

    # Create year/month only if they are needed
    # for partitioning.
    if partition_source:

        if "year" in partition_columns:
            df = df.withColumn(
                "year",
                F.year(F.col(partition_source))
            )

        if "month" in partition_columns:
            df = df.withColumn(
                "month",
                F.month(F.col(partition_source))
            )

    writer = (
        df.write
        .format("delta")
        .mode("overwrite")
    )

    if partition_columns:
        writer = writer.partitionBy(
            *partition_columns
        )

    writer.save(config["silver_path"])

    print(f"-> Silver saved in {config['silver_path']}")


# ============================================================
# INGESTION METADATA
# ============================================================

def write_metadata(spark: SparkSession, dataset_name: str, config: dict, processed: int, rejected: int, execution_time: float):
    metadata = [{
        "dataset_name": dataset_name,
        "schema_version": config.get("schema_version", 1),
        "processed_records": processed,
        "rejected_records": rejected,
        "execution_time_seconds": execution_time
    }]

    metadata_df = spark.createDataFrame(metadata).withColumn("ingestion_timestamp", F.current_timestamp())
    metadata_path = config.get("metadata_path", "storage/metadata")
    metadata_df.write.format("delta").mode("append").save(metadata_path)


# ============================================================
# COMPLETE INGESTION
# ============================================================

def ingest_data(spark: SparkSession, dataset_name: str, config: dict):
    print(f"\n--- Ingesting {dataset_name} ---")
    start = time.perf_counter()

    # Read & Bronze
    df = read_dataset(spark, config)
    processed = df.count()
    df = validate_and_rename(df, config)
    write_to_bronze(df, config)

    # Silver Processing
    df = clean_strings(df).withColumn("_is_valid", F.lit(True))
    df = cast_types(df, config)
    df = build_timestamps(df, config)
    df = apply_quality_rules(df, config)

    # Deduplicate & Metrics
    pk = config.get("primary_key", [])
    valid_df = deduplicate_records(df.filter(F.col("_is_valid")).drop("_is_valid"), pk)
    valid = valid_df.count()
    rejected = processed - valid

    # Write Silver & Metadata
    write_to_silver(valid_df, config)
    execution_time = time.perf_counter() - start
    write_metadata(spark, dataset_name, config, processed, rejected, execution_time)

    # Summary
    print(f"Processed: {processed}\nValid: {valid}\nRejected: {rejected}\nTime: {execution_time:.2f}s")