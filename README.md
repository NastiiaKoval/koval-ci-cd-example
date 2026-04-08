# lab7_bundle_ci/cd

# lab7 — Medallion ETL Pipeline with CI/CD

This project implements a **Medallion Architecture** (Raw → Bronze → Silver → Gold)
using Databricks Asset Bundles (DAB), Delta Live Tables (DLT), and GitHub Actions CI/CD.
It automates deployment of ETL code across DEV and PROD environments on Azure Databricks.

---

## Project Structure

├── .github/
│   └── workflows/
│       ├── ci.yml          # Validates bundle on every Pull Request to main
│       └── cd.yml          # Deploys DEV → PROD on merge to main
├── src/
│   ├── 00_generate_data.py     # Generates synthetic JSON event files to ADLS
│   ├── 01_source_to_bronze.py  # Auto Loader ingestion → raw + bronze DLT tables
│   ├── 02_bronze_to_silver.py  # DLT Expectations, dedup, cleansing → silver
│   └── 03_silver_to_gold.py    # Aggregations → gold (sessions, daily stats)
├── databricks.yml              # Bundle config: variables, dev + prod targets
├── pyproject.toml
└── README.md

---

## CI/CD Pipeline

Every code change goes through an automated pipeline before reaching production:
feature/* branch
↓  Pull Request
CI: bundle validate --target dev     ← blocks merge if invalid
↓  Merge to main
CD: bundle deploy + run --target dev
↓  on success
CD: bundle deploy + run --target prod

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
databricks bundle deploy --target prod
```

To run the pipeline after deployment:
```bash
databricks bundle run lab7_daily_job --target prod
```

---

## Configuration and Parameters

Key parameters defined in `databricks.yml` and injected via `spark.conf.get()`
**Secret scopes**: Must exist in each workspace independently before first deploy.
  PROD requires `kvbddev-scope/storagekey` configured in the PROD workspace.
