# urban-data-integration-platform

This project implements a generic Spark-based ingestion and integration framework for heterogeneous urban datasets using Delta Lake.

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

python3 -m venv .venv

Activate it on Linux/macOS:

source .venv/bin/activate

On Windows:

spark_env\Scripts\activate

Install the required dependencies:

pip install -r requirements.txt

Finally, run the framework:

python main.py

The framework will automatically read the configuration files, ingest and validate the datasets, write the Bronze and Silver Delta tables, and build the final integrated Gold table.