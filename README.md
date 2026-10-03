```markdown
# urban-data-integration-platform

This project implements a generic Spark-based ingestion, integration, validation, monitoring, analytics, and machine learning framework for heterogeneous urban datasets using Delta Lake.

## How to Run

Place the raw datasets inside the `raw/` directory, using one subfolder for each dataset:

```text
raw/
├── taxi_trips/
├── weather/
├── air_quality/
└── taxi_zones/
```

Create the corresponding YAML configuration files inside the `configs/` directory.

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

Run the complete data pipeline with:

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
- updates reusable analytical data products,
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

Run the analytical queries and optimization benchmarks with:

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

## Machine Learning

The platform includes a Spark ML pipeline for predicting hourly taxi demand.

The prediction target is:

```text
taxi_demand
```

which represents the number of taxi pickups in a specific pickup zone during a specific hour.

The ML dataset is automatically generated from:

```text
storage/gold/integrated_taxi_trips/
```

Each row represents one pickup zone during one hour.

The model uses features including:

- hour of day,
- day of week,
- month,
- pickup zone,
- pickup borough,
- temperature,
- relative humidity,
- precipitation,
- air-quality measurements.

The feature configuration is stored in:

```text
ml/configs/taxi_demand.yaml
```

### Feature Engineering

The reusable Spark ML feature pipeline automatically performs:

- missing-value imputation using the median,
- categorical indexing using `StringIndexer`,
- categorical encoding using `OneHotEncoder`,
- feature vector creation using `VectorAssembler`.

The dataset is split chronologically into training, validation, and test sets.

For the current dataset:

```text
Training:   January - April
Validation: May
Test:       June
```

The current model uses a `RandomForestRegressor` with:

```text
numTrees = 50
maxDepth = 8
seed = 42
```

### Train and Evaluate the Model

Run the ML training pipeline with:

```bash
python3 -m src.ml.main
```

The pipeline automatically:

- builds the ML dataset from the Gold table,
- creates temporal features,
- splits the dataset,
- applies feature engineering,
- trains the Random Forest model,
- evaluates validation and test datasets,
- reports RMSE, MAE, and R²,
- reports execution times,
- saves the complete trained Spark ML pipeline.

The trained model is stored in:

```text
storage/models/taxi_demand/
```

The complete Spark `PipelineModel` is saved, including both feature preprocessing and the trained regression model.

### Model Evaluation

The current ML dataset contains:

```text
363,160 rows
```

with the following split:

```text
Training:   279,108 rows
Validation:  40,772 rows
Test:        43,280 rows
```

The final model produced approximately:

```text
Validation RMSE: 10.63
Validation MAE:   8.20
Validation R²:   -0.3215

Test RMSE:       10.23
Test MAE:         7.77
Test R²:         -0.1331
```

The objective of the project is not to maximize prediction accuracy, but to demonstrate a reusable and reproducible Spark ML workflow.

Increasing the Random Forest complexity significantly improved the model compared with the initial configuration. Further improvements could include historical-demand features such as previous-hour demand or average demand for the same zone and hour.

### Run Predictions

A saved model can be loaded and used without retraining.

Run example predictions with:

```bash
python3 -m src.ml.predict
```

The prediction script loads:

```text
storage/models/taxi_demand/
```

and applies the complete saved pipeline to new feature values.

Example inputs include:

```text
pickup zone
pickup borough
hour of day
day of week
month
temperature
humidity
precipitation
air quality
```

The output is the predicted hourly taxi demand.

### Retraining

Retraining uses the same command:

```bash
python3 -m src.ml.main
```

When new data has been processed by the platform, the ML dataset is rebuilt from the updated Gold table and the same feature engineering and training pipeline is executed again.

No separate preprocessing implementation is required.

## Raw Data vs Integrated Platform Comparison

The project also compares two workflows for creating the same ML dataset.

### Approach A: Raw Data

Approach A starts directly from the original Taxi Trips, Weather, Air Quality, and Taxi Zones files.

It manually performs:

- loading,
- schema conversion,
- timestamp construction,
- cleaning,
- validation,
- dataset integration,
- aggregation,
- feature engineering,
- model training.

Run the experiment with:

```bash
python3 -m src.ml.approach_a
```

### Approach B: Integrated Platform

Approach B uses the integrated Gold dataset produced by the platform.

Most schema handling, validation, cleaning, and integration have already been completed before the ML workflow starts.

The comparison produced:

| Metric | Approach A: Raw | Approach B: Platform |
| --- | ---: | ---: |
| ML dataset rows | 363,160 | 363,160 |
| Data preparation | 34.99 s | 16.06 s |
| Model training | 120.98 s | 110.15 s |
| Total execution | 165.08 s | 138.35 s |
| Test RMSE | 9.81 | 10.23 |
| Test MAE | 7.31 | 7.77 |
| Test R² | -0.0422 | -0.1331 |

Using the integrated platform reduced data preparation time by approximately 54%.

More importantly, Approach B avoids repeating schema conversion, validation, timestamp processing, and integration logic inside the ML workflow.

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
├── models/
│   └── taxi_demand/
│
├── rejected/
├── metadata/
└── processed_files/
```

The layers have the following roles:

- **Bronze:** raw ingested data with schema evolution support.
- **Silver:** validated, cleaned, deduplicated, and standardized datasets.
- **Gold:** integrated analytical data and reusable data products.
- **Models:** trained Spark ML pipelines.
- **Rejected:** invalid records isolated during validation.
- **Metadata:** operational monitoring information for pipeline executions.
- **Processed Files:** tracks already processed input files for incremental ingestion.
```