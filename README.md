```markdown
# urban-data-integration-platform

This project implements a generic Spark-based ingestion, integration, validation, monitoring, and analytics framework for heterogeneous urban datasets using Delta Lake.

## How to Run

Place the raw datasets inside the `raw/` directory, using one subfolder for each dataset:

```text
raw/
├── taxi_trips/
├── weather/
├── air_quality/
└── taxi_zones/
```

Then create the corresponding YAML configuration files inside the `configs/` directory.

Each configuration can define:

- input format,
- schema and column names,
- data types,
- primary keys,
- quality rules,
- timestamps,
- partitioning strategy,
- schema version,
- reference validation rules,
- incremental update behaviour.

Create a Python virtual environment:

```bash
python3 -m venv .venv
```

Activate it on Linux/macOS:

```bash
source .venv/bin/activate
```

On Windows:

```bash
.venv\Scripts\activate
```

Install the required dependencies:

```bash
pip install -r requirements.txt
```

Run the complete pipeline with:

```bash
python main.py
```

The framework automatically:

- detects new input files,
- ingests data into the Bronze layer,
- validates and standardizes records,
- detects duplicates and invalid records,
- validates reference keys,
- isolates rejected records,
- incrementally updates Silver Delta tables,
- handles schema evolution,
- refreshes the integrated Gold dataset,
- updates the reusable analytical data products,
- records execution metadata for monitoring.

## Incremental Processing

Processed files are tracked in:

```text
storage/processed_files/
```

Only new input files are processed during subsequent executions.

Silver tables are updated incrementally using Delta Lake `MERGE`, allowing new records to be inserted and existing records to be updated when required.

The Gold layer can refresh only the affected year/month partitions when possible, avoiding unnecessary full recomputation.

## Data Validation

The validation framework supports:

- duplicate detection,
- invalid attribute values,
- missing or invalid timestamps,
- incomplete records,
- missing reference records,
- unsupported schema changes,
- configurable numeric ranges and null constraints.

Validation rules are mainly defined through the YAML configuration files.

For example, Taxi Trips can validate pickup and dropoff location IDs against the Taxi Zones Silver table.

Invalid records are excluded from the Silver and Gold analytical layers and stored separately under:

```text
storage/rejected/
```

Example:

```text
storage/rejected/
└── taxi_trips/
```

## Monitoring

Every pipeline execution automatically stores operational metadata in a Delta table:

```text
storage/metadata/
```

Recorded information includes:

- dataset name,
- schema version,
- processed records,
- inserted records,
- duplicate records,
- rejected records,
- validation failures,
- execution time,
- execution status,
- error message when available.

Run the monitoring queries with:

```bash
python3 -m src.monitoring
```

The monitoring module reports:

- datasets with the most validation failures,
- datasets with the longest processing times,
- rejected records per execution,
- processing-time trends,
- execution summaries,
- failed executions.

## Analytics and Benchmarks

To run the analytical queries and optimization benchmarks:

```bash
python3 -m src.analytics.main
```

The analytics module evaluates techniques such as:

- caching,
- Adaptive Query Execution (AQE),
- partition pruning,
- broadcast joins.

It also reports query execution times and execution plans.

## Reusable Data Products

Reusable analytical data products are generated from the integrated Gold dataset and stored under:

```text
storage/gold/data_products/
```

The generated products include:

```text
data_products/
├── daily_mobility_summary/
├── taxi_zone_statistics/
├── weather_impact_summary/
├── quality_impact_summary/
└── borough_mobility_summary/
```

The data products provide pre-aggregated information for common analytical workloads.

They also contain metadata such as:

- data source,
- creation time,
- refresh time,
- schema version.

Where possible, data products are refreshed only when their source datasets are affected.

## Output Structure

The platform storage is organized as:

```text
storage/
├── bronze/
│   ├── taxi_trips/
│   ├── weather/
│   ├── air_quality/
│   └── taxi_zones/
│
├── silver/
│   ├── taxi_trips/
│   ├── weather/
│   ├── air_quality/
│   └── taxi_zones/
│
├── gold/
│   ├── integrated_taxi_trips/
│   └── data_products/
│       ├── daily_mobility_summary/
│       ├── taxi_zone_statistics/
│       ├── weather_impact_summary/
│       ├── quality_impact_summary/
│       └── borough_mobility_summary/
│
├── rejected/
├── metadata/
└── processed_files/
```

The layers have the following roles:

- **Bronze:** raw ingested data with schema evolution support.
- **Silver:** validated, cleaned, deduplicated, and standardized datasets.
- **Gold:** integrated analytical data and reusable data products.
- **Rejected:** invalid records isolated during validation.
- **Metadata:** operational monitoring information for pipeline executions.
- **Processed Files:** tracks already processed input files for incremental ingestion.
```