import os
import re
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, LongType, DoubleType


PROCESSED_FILES_PATH = "storage/processed_files"
REJECTED_RECORDS_PATH = "storage/rejected"


# ============================================================
# READ DATASET + INCREMENTAL FILE DISCOVERY
# ============================================================

def get_raw_files(config: dict) -> list:
    path = config["input_path"]
    extension = ".csv" if config["format"].lower() == "csv" else ".parquet"
    return [os.path.join(root, filename) for root, _, files in os.walk(path) for filename in files if filename.endswith(extension)]


def get_new_files(spark: SparkSession, dataset_name: str, config: dict) -> list:
    all_files = get_raw_files(config)

    if not DeltaTable.isDeltaTable(spark, PROCESSED_FILES_PATH):
        return all_files

    processed_df = spark.read.format("delta").load(PROCESSED_FILES_PATH).filter(F.col("dataset_name") == dataset_name)
    processed_files = {row["file_path"] for row in processed_df.select("file_path").collect()}
    return [file for file in all_files if file not in processed_files]


def read_dataset(spark: SparkSession, config: dict, paths: list) -> DataFrame:
    fmt = config["format"].lower()

    if fmt == "csv":
        dfs = [spark.read.option("header", config.get("header", True)).option("delimiter", config.get("delimiter", ",")).csv(path) for path in paths]
        df = dfs[0]

        for other_df in dfs[1:]:
            df = df.unionByName(other_df, allowMissingColumns=True)

        return df

    if fmt == "parquet":
        return spark.read.parquet(*paths)

    raise ValueError(f"Unsupported format: {fmt}")


def mark_files_as_processed(spark: SparkSession, dataset_name: str, files: list):
    rows = [(dataset_name, file) for file in files]
    df = spark.createDataFrame(rows, ["dataset_name", "file_path"]).withColumn("processed_at", F.current_timestamp())
    df.write.format("delta").mode("append").save(PROCESSED_FILES_PATH)


# ============================================================
# SCHEMA VALIDATION + COLUMN RENAMING
# ============================================================

def validate_and_rename(df: DataFrame, config: dict) -> DataFrame:
    columns_cfg = config.get("columns", {})
    expected = set(columns_cfg.keys())
    actual = set(df.columns)

    required = {name for name, meta in columns_cfg.items() if not meta.get("optional", False)}
    missing_required = required - actual
    extra = actual - expected

    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    if extra:
        raise ValueError(f"Unsupported columns: {extra}")

    # Add missing optional columns as NULL
    for source_name, meta in columns_cfg.items():
        if source_name not in df.columns and meta.get("optional", False):
            df = df.withColumn(source_name, F.lit(None).cast("string"))

    snake_case_pattern = r"^[a-z][a-z0-9_]*$"

    for source_name, meta in columns_cfg.items():
        target_name = meta.get("target", source_name)

        if not re.fullmatch(snake_case_pattern, target_name):
            raise ValueError(f"Column '{target_name}' is not snake_case")

        if source_name != target_name:
            df = df.withColumnRenamed(source_name, target_name)

    return df


# ============================================================
# BRONZE
# ============================================================

def write_to_bronze(df: DataFrame, config: dict):
    df.write.format("delta").mode("append").option("mergeSchema", "true").save(config["bronze_path"])
    print(f"-> Bronze updated: {config['bronze_path']}")


# ============================================================
# CLEAN STRINGS
# ============================================================

def clean_strings(df: DataFrame) -> DataFrame:
    null_values = ["", "NA", "N/A", "NULL"]

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            cleaned = F.trim(F.col(field.name))
            df = df.withColumn(field.name, F.when(cleaned.isin(*null_values), F.lit(None)).otherwise(cleaned))

    return df


# ============================================================
# TYPE CASTING
# ============================================================

def cast_types(df: DataFrame, config: dict) -> DataFrame:
    for source_name, meta in config.get("columns", {}).items():
        target_name = meta.get("target", source_name)
        target_type = meta.get("type")

        if not target_type:
            continue

        original = F.col(target_name)
        casted = original.cast(target_type)
        invalid_cast = original.isNotNull() & casted.isNull()

        df = df.withColumn("_is_valid", F.col("_is_valid") & ~invalid_cast)
        df = df.withColumn(target_name, casted)

    return df


# ============================================================
# TIMESTAMP CREATION
# ============================================================

def build_timestamps(df: DataFrame, config: dict) -> DataFrame:
    for target_col, meta in config.get("timestamp_columns", {}).items():
        mode = meta["mode"]
        cols = meta["source_columns"]

        if mode == "components":
            timestamp = F.make_timestamp(F.col(cols[0]), F.col(cols[1]), F.col(cols[2]), F.col(cols[3]), F.lit(0), F.lit(0))
        elif mode == "concat":
            value = F.concat_ws(" ", *[F.col(c) for c in cols])
            timestamp = F.to_timestamp(value, meta["format"])
        else:
            raise ValueError(f"Unknown timestamp mode: {mode}")

        df = df.withColumn(target_col, timestamp)

    return df


# ============================================================
# PRIMARY KEY
# ============================================================

def ensure_primary_key(df: DataFrame, config: dict) -> DataFrame:
    primary_key = config.get("primary_key", [])

    if not primary_key:
        return df

    missing = [col for col in primary_key if col not in df.columns]

    if not missing:
        return df

    if not config.get("generate_primary_key", False):
        raise ValueError(f"Primary key columns missing: {missing}")

    if len(primary_key) != 1:
        raise ValueError("Generated primary key must contain exactly one column")

    pk_name = primary_key[0]
    hash_columns = [c for c in df.columns if c != pk_name and not c.startswith("_")]

    return df.withColumn(pk_name, F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<NULL>")) for c in hash_columns]), 256))


# ============================================================
# DATA QUALITY
# ============================================================

def apply_quality_rules(df: DataFrame, config: dict) -> DataFrame:
    quality = config.get("quality_rules", {})

    # Primary keys cannot be NULL
    for col_name in config.get("primary_key", []):
        df = df.withColumn("_is_valid", F.col("_is_valid") & F.col(col_name).isNotNull())

    # Required columns cannot be NULL
    for col_name in quality.get("not_null", []):
        df = df.withColumn("_is_valid", F.col("_is_valid") & F.col(col_name).isNotNull())

    # Required timestamps must be valid
    for col_name in quality.get("required_timestamps", []):
        df = df.withColumn("_is_valid", F.col("_is_valid") & F.col(col_name).isNotNull())

    # Validate numeric ranges
    for col_name, bounds in quality.get("numeric_ranges", {}).items():
        if "min" in bounds:
            df = df.withColumn("_is_valid", F.col("_is_valid") & (F.col(col_name).isNull() | (F.col(col_name) >= bounds["min"])))

        if "max" in bounds:
            df = df.withColumn("_is_valid", F.col("_is_valid") & (F.col(col_name).isNull() | (F.col(col_name) <= bounds["max"])))

    return df


# ============================================================
# REFERENCE VALIDATION
# ============================================================

def apply_reference_rules(spark: SparkSession, df: DataFrame, config: dict) -> DataFrame:
    for rule in config.get("reference_rules", []):
        column = rule["column"]
        reference_path = rule["reference_path"]
        reference_column = rule["reference_column"]

        if not DeltaTable.isDeltaTable(spark, reference_path):
            raise ValueError(f"Reference table not found: {reference_path}")

        reference_values = [
            row[reference_column]
            for row in spark.read.format("delta").load(reference_path).select(reference_column).distinct().collect()
        ]

        df = df.withColumn(
            "_is_valid",
            F.col("_is_valid") & (F.col(column).isNull() | F.col(column).isin(reference_values))
        )

    return df

# ============================================================
# REJECTED RECORDS
# ============================================================

def write_rejected_records(df: DataFrame, dataset_name: str):
    path = f"{REJECTED_RECORDS_PATH}/{dataset_name}"
    df.filter(~F.col("_is_valid")).drop("_is_valid").write.format("delta").mode("append").option("mergeSchema", "true").save(path)


# ============================================================
# DUPLICATES
# ============================================================

def deduplicate_records(df: DataFrame, primary_key: list) -> DataFrame:
    return df.dropDuplicates(primary_key) if primary_key else df.dropDuplicates()


# ============================================================
# DETECT CHANGED RECORDS
# ============================================================

def get_changed_records(spark: SparkSession, dataset_name: str, df: DataFrame, config: dict) -> DataFrame:
    path = config["silver_path"]

    if not DeltaTable.isDeltaTable(spark, path):
        return df

    # Taxi exact duplicates already present in Silver are ignored
    if dataset_name == "taxi_trips":
        primary_key = config.get("primary_key", [])
        existing_keys = spark.read.format("delta").load(path).select(*primary_key)
        return df.join(existing_keys, on=primary_key, how="left_anti")

    # Weather and Air Quality may contain corrected records
    return df


def get_affected_partitions(spark: SparkSession, dataset_name: str, df: DataFrame, config: dict) -> set:
    if dataset_name == "taxi_trips":
        timestamp = F.col("tpep_pickup_datetime")

        if DeltaTable.isDeltaTable(spark, config["silver_path"]):
            max_timestamp = spark.read.format("delta").load(config["silver_path"]).agg(F.max("tpep_pickup_datetime")).first()[0]

            if max_timestamp is not None:
                df = df.filter(F.col("tpep_pickup_datetime") > F.lit(max_timestamp))

        partitions = df.select(F.year(timestamp).alias("year"), F.month(timestamp).alias("month"))

    elif dataset_name == "weather":
        partitions = df.select(F.col("year").cast("int").alias("year"), F.col("month").cast("int").alias("month"))

    elif dataset_name == "air_quality":
        date = F.to_date("date_local", "yyyy-MM-dd")
        partitions = df.select(F.year(date).alias("year"), F.month(date).alias("month"))

    else:
        return set()

    rows = partitions.filter(F.col("year").isNotNull() & F.col("month").isNotNull()).distinct().collect()
    return {(row["year"], row["month"]) for row in rows}


# ============================================================
# SILVER INCREMENTAL MERGE
# ============================================================

def write_to_silver(spark: SparkSession, df: DataFrame, config: dict) -> int:
    path = config["silver_path"]
    primary_key = config.get("primary_key", [])
    partition_columns = config.get("partition_columns", [])
    partition_source = config.get("partition_source_timestamp")

    if partition_source:
        if "year" in partition_columns:
            df = df.withColumn("year", F.year(F.col(partition_source)))

        if "month" in partition_columns:
            df = df.withColumn("month", F.month(F.col(partition_source)))

    # First build
    if not DeltaTable.isDeltaTable(spark, path):
        writer = df.write.format("delta").mode("overwrite")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.save(path)
        return df.count()

    if not primary_key:
        raise ValueError("Incremental merge requires a primary key")

    delta_table = DeltaTable.forPath(spark, path)
    condition = " AND ".join(f"old.`{col}` = new.`{col}`" for col in primary_key)

    merge = delta_table.alias("old").merge(df.alias("new"), condition).withSchemaEvolution()

    # Natural-key datasets can update existing records
    if config.get("update_existing", True):
        merge = merge.whenMatchedUpdateAll()

    merge.whenNotMatchedInsertAll().execute()

    metrics = delta_table.history(1).select("operationMetrics").first()["operationMetrics"]
    return int(metrics.get("numTargetRowsInserted", 0))


# ============================================================
# MONITORING METADATA
# ============================================================

def write_metadata(
    spark: SparkSession,
    dataset_name: str,
    config: dict,
    processed: int,
    inserted: int,
    duplicates: int,
    rejected: int,
    execution_time: float,
    status: str = "success",
    error_message: str = None
):
    validation_failures = rejected

    schema = StructType([
        StructField("dataset_name", StringType(), False),
        StructField("schema_version", LongType(), False),
        StructField("processed_records", LongType(), False),
        StructField("inserted_records", LongType(), False),
        StructField("duplicate_records", LongType(), False),
        StructField("rejected_records", LongType(), False),
        StructField("validation_failures", LongType(), False),
        StructField("execution_time_seconds", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True)
    ])

    metadata = [(
        dataset_name,
        int(config.get("schema_version", 1)),
        int(processed),
        int(inserted),
        int(duplicates),
        int(rejected),
        int(validation_failures),
        float(execution_time),
        status,
        error_message
    )]

    metadata_df = spark.createDataFrame(metadata, schema).withColumn("ingestion_timestamp", F.current_timestamp())
    metadata_path = config.get("metadata_path", "storage/metadata")
    metadata_df.write.format("delta").mode("append").option("mergeSchema", "true").save(metadata_path)


# ============================================================
# COMPLETE INGESTION
# ============================================================

def ingest_data(spark: SparkSession, dataset_name: str, config: dict):
    print(f"\n--- Ingesting {dataset_name} ---")
    start = time.perf_counter()

    processed = 0
    inserted = 0
    duplicates = 0
    rejected = 0

    try:
        new_files = get_new_files(spark, dataset_name, config)

        if not new_files:
            execution_time = time.perf_counter() - start
            write_metadata(spark, dataset_name, config, 0, 0, 0, 0, execution_time, status="no_new_files")
            print("No new files to process.")
            return set(), False

        print(f"New files: {len(new_files)}")

        df = read_dataset(spark, config, new_files)
        processed = df.count()

        df = validate_and_rename(df, config)

        # Detect affected partitions while the batch plan is still simple
        affected_partitions = get_affected_partitions(spark, dataset_name, df, config)

        write_to_bronze(df, config)

        df = clean_strings(df).withColumn("_is_valid", F.lit(True))
        df = cast_types(df, config)
        df = build_timestamps(df, config)
        df = ensure_primary_key(df, config)
        df = apply_quality_rules(df, config)

        # Validate references defined in the YAML configuration
        df = apply_reference_rules(spark, df, config)

        valid_before_dedup = df.filter(F.col("_is_valid")).drop("_is_valid")
        valid_count = valid_before_dedup.count()
        rejected = processed - valid_count

        # Isolate rejected records
        if rejected > 0:
            write_rejected_records(df, dataset_name)

        pk = config.get("primary_key", [])
        valid_df = deduplicate_records(valid_before_dedup, pk)
        valid = valid_df.count()

        # Duplicates inside the current batch
        duplicates = valid_count - valid

        # Delta MERGE handles existing records directly
        inserted = write_to_silver(spark, valid_df, config)

        # For Taxi, records not inserted were already present in Silver
        if dataset_name == "taxi_trips":
            duplicates += valid - inserted

        execution_time = time.perf_counter() - start

        write_metadata(spark, dataset_name, config, processed, inserted, duplicates, rejected, execution_time)
        mark_files_as_processed(spark, dataset_name, new_files)

        print(
            f"Processed: {processed}\n"
            f"Valid: {valid}\n"
            f"Inserted: {inserted}\n"
            f"Duplicates: {duplicates}\n"
            f"Rejected: {rejected}\n"
            f"Validation failures: {rejected}\n"
            f"Time: {execution_time:.2f}s"
        )

        full_gold_refresh = dataset_name == "taxi_zones" and valid > 0
        return affected_partitions, full_gold_refresh

    except Exception as e:
        execution_time = time.perf_counter() - start

        try:
            write_metadata(spark, dataset_name, config, processed, inserted, duplicates, rejected, execution_time, status="failed", error_message=str(e)[:1000])
        except Exception:
            pass

        raise