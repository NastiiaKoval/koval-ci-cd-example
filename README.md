# lab5_bundle

This project implements a complete Medallion Architecture (Raw, Bronze, Silver, Gold) using **Databricks Asset Bundles (DABs)** and **Delta Live Tables (DLT)**. It automates the deployment of infrastructure and ETL code for processing event data within an Azure Databricks environment.

## Project Structure

* **`resources/`**: Infrastructure configuration files (YAML).
    * `lab5_bundle_etl.pipeline.yml`: Defines the DLT pipeline settings (clusters, libraries, and target schemas).
    * `lab5_job.job.yml`: Defines the Databricks Job that triggers the pipeline on a schedule.
* **`src/`**: Python source code for data processing.
    * `00_source_to_bronze.py`: Ingests raw JSON data from Azure Data Lake Storage using Auto Loader.
    * `01_bronze_to_silver.py`: Handles data cleansing, validation via DLT Expectations, and deduplication.
    * `02_silver_to_gold.py`: Performs data aggregation and business-level transformations.
* **`databricks.yml`**: The main bundle configuration file defining environment variables (catalog, schema, paths) for `dev` and `prod` targets.

---

## Getting Started

### 1. Prerequisites
Ensure you have the **uv** package manager installed on your system:
```bash
brew install uv
```

Install project dependencies (including `databricks-dlt` for local development support):
```bash
uv sync --dev
```

### 2. Authentication
Configure the Databricks CLI to connect to your Azure Databricks workspace:
```bash
databricks configure
```

### 3. Deployment
Deploy a development copy of the project. This will prefix resources with `[dev <user_name>]` and point to your development schema:
```bash
databricks bundle deploy
```

To run the pipeline after deployment:
```bash
databricks bundle run lab5_bundle_etl
```

---

## Configuration and Parameters

To avoid hardcoding sensitive information or environment-specific paths, this project utilizes bundle variables defined in `databricks.yml`. Key parameters include:

* **Catalog**: `dbr_dev`
* **Target Schema**: `koval_bronze`
* **Source Path**: `abfss://kovalcontainer@sadlsdev.dfs.core.windows.net/raw/incremental/`

These values are injected into the Spark configuration of the pipeline and accessed within Python scripts via `spark.conf.get()`.
