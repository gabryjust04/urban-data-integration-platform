# urban-data-integration-platform

This project implements a generic Spark-based ingestion, integration, and analytics framework for heterogeneous urban datasets using Delta Lake.

## How to Run

Place the raw datasets inside the `raw/` directory, using one subfolder for each dataset. For example:

```text
raw/
├── taxi_trips/
├── weather/
├── air_quality/
└── taxi_zones/
```

Then create the corresponding YAML configuration files inside the `configs/` directory.

Each configuration defines the input format, schema, column names, data types, quality rules, timestamps, and partitioning strategy.

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
spark_env\Scripts\activate
```

Install the required dependencies:

```bash
pip install -r requirements.txt
```

Run the ingestion and integration framework with:

```bash
python main.py
```

The framework will automatically read the configuration files, ingest and validate the datasets, write the Bronze and Silver Delta tables, and build the final integrated Gold table.

## Analytics

To run the analytical queries, optimization benchmarks, and reusable data products:

```bash
python3 -m src.analytics.main
```

The module evaluates caching, AQE, partition pruning, and broadcast joins, and reports execution times and query plans.

## Reusable Data Products

The analytical module automatically generates reusable analytical data products from the integrated Gold dataset and stores them as Delta tables under:

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

Each data product contains pre-aggregated analytical information together with metadata such as:

* data source,
* creation time,
* refresh time,
* schema version.

These materialized tables allow analysts to execute simple queries without repeatedly scanning and aggregating the complete integrated dataset.

## Output Structure

The storage layers are organized as:

```text
storage/
├── bronze/
├── silver/
└── gold/
    ├── integrated_taxi_trips/
    └── data_products/
```

The Bronze layer contains the ingested raw data, the Silver layer contains validated and standardized datasets, and the Gold layer contains the integrated analytical dataset and reusable data products.
