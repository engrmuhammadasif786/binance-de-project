# Binance Market Analytics — DE Zoomcap Capstone Project

An end-to-end data pipeline that ingests daily OHLCV candlestick data from the
public Binance REST API, stores it in a GCP data lake (GCS), loads it into a
BigQuery data warehouse, transforms it with dbt, and powers a Looker Studio
dashboard.

**Dashboard:** https://binance-de-project.streamlit.app/

## Problem statement

Cryptocurrency markets move fast, and understanding cross-coin volume, price
trends, and volatility requires a reliable, reproducible analytics pipeline.
This project builds a fully orchestrated batch pipeline that:

1. pulls daily trading data for 10 major USDT pairs from Binance,
2. lands it in a cloud data lake (GCS),
3. loads it into a partitioned & clustered BigQuery warehouse,
4. transforms raw candles into analytics-ready daily metrics (returns, volatility) with dbt,
5. visualizes the results in an interactive dashboard (volume by pair, price/volatility over time).

## Architecture

```
Binance REST API (/api/v3/klines, no key required)
        |
        v
[Airflow DAG: binance_daily_pipeline]  (Composer or Docker, scheduled @daily)
        |
        v
GCS data lake:  gs://<bucket>/raw/candles/symbol=<SYMBOL>/date=<YYYY-MM-DD>/data.parquet
        |
        v
BigQuery:  binance_analytics.raw_candles
           PARTITION BY open_date   (dashboard always filters by time range)
           CLUSTER  BY symbol       (queries filter/join by trading pair)
        |
        v
dbt:  stg_candles (view) -> fact_daily_metrics (table: returns, 7d volatility)
        |
        v
Looker Studio dashboard (streamlit)
```

## Tech stack

| Layer | Technology |
|---|---|
| Cloud | GCP |
| IaC | Terraform |
| Orchestration | Airflow (TaskFlow/PythonOperator + Google provider operators) |
| Data lake | Google Cloud Storage (parquet, hive-style partitions) |
| Data warehouse | BigQuery |
| Transformations | dbt (dbt-bigquery) |
| Dashboard | Looker Studio / Streamlit |
| Ingestion | Python (`requests`, `pandas`, `pyarrow`) |

## Repository layout

```
.
├── dags/binance_pipeline.py     # end-to-end Airflow DAG
├── scripts/ingest_binance.py    # Binance API client with retries
├── dbt/binance/                 # dbt project (staging + marts)
├── terraform/                   # GCS bucket, BQ dataset, service account
├── docker/                      # Dockerfile + requirements for local Airflow
├── docker-compose.yml           # local Airflow (webserver + scheduler + postgres)
└── .env.example                 # environment template
```

## How to run

### 0. Prerequisites
- A GCP project with billing enabled
- `gcloud` CLI, `terraform`, `docker`, `docker compose` installed
- Clone this repo

### 1. Provision infrastructure with Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars   # fill in your values
terraform init
terraform apply
terraform output -raw airflow_sa_key_base64 | base64 -d > ../gcp-key.json
```

This creates the GCS data-lake bucket, the BigQuery dataset, and a service
account for Airflow. The key is written to `gcp-key.json` (git-ignored).

### 2. Configure the environment

```bash
cp .env.example .env   # fill in GCP_PROJECT, GCS_BUCKET, path to gcp-key.json
source .env
```

### 3. Option A — run Airflow locally with Docker (recommended, cheapest)

```bash
docker compose up --build -d
# open http://localhost:8080  (default login: airflow / airflow)
```

Trigger a backfill to load history (e.g. last year of daily candles):

```bash
docker compose exec scheduler airflow dags backfill binance_daily_pipeline   -s 2025-10-01 -e 2026-09-30
```

### 3. Option B — Google Cloud Composer

Create a Composer environment in the GCP console (or via `gcloud`), upload
`dags/`, `scripts/`, and `dbt/binance/` to the environment's bucket, set the
environment variables from `.env`, and run the same backfill command via
`gcloud composer environments run`.

### 4. Verify

```bash
bq query --use_legacy_sql=false   'SELECT symbol, open_date, close FROM `'"$GCP_PROJECT"'.binance_analytics.fact_daily_metrics`
   ORDER BY open_date DESC LIMIT 10'
```

### 5. Dashboard

Connect Looker Studio to the BigQuery dataset `binance_analytics` and build
at least these tiles (see `dbt/binance/models/marts/fact_daily_metrics.sql`):

1. **Bar chart** — 24h quote volume by trading pair (categorical distribution)
2. **Time series** — closing price (or daily return %) by symbol over time
3. *(extra)* — 7-day rolling volatility by symbol

### Alternative: Streamlit dashboard (code-defined, interactive)

A fully interactive Streamlit version of the dashboard ships in `dashboards/` —
metric switcher, date range, multi-symbol filter, moving-average smoothing,
KPI cards, volume ranking, volatility heatmap, monthly table and top movers.

```bash
cd dashboards
pip install -r requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp-key.json
export GCP_PROJECT=your-project-id
streamlit run streamlit_app.py
```

## Design decisions

- **Batch, not stream**: daily candles are complete, immutable records; a daily
  scheduled batch is simpler, cheaper, and fully reproducible via backfill.
- **Parquet in GCS**: columnar, compressed, schema-preserving — and files are
  partitioned `symbol=/date=` for cheap re-processing.
- **Partition `raw_candles` by `open_date`, cluster by `symbol`**: every
  dashboard query filters on a time window (partition pruning) and/or a subset
  of pairs (cluster pruning), so BigQuery scans a fraction of the bytes.
- **dbt for transformations**: versioned, tested, documented SQL models instead
  of ad-hoc queries; `fact_daily_metrics` is materialized as a table for fast
  dashboard reads.
- **Retries in the ingestion script**: Binance's public API rate-limits (HTTP 429)
  and occasionally returns 5xx; exponential backoff plus per-symbol error
  isolation keeps the DAG robust.

## Known limitations / future work

- Daily interval only (hourly would need larger backfills)
- No tests or CI/CD yet (see "going the extra mile")
- `max_active_runs=1` serializes daily runs to protect the public API

## Reproducibility checklist

- [ ] `terraform apply` works from a clean clone
- [ ] `docker compose up` starts without errors
- [ ] backfill completes end-to-end with no manual steps
- [ ] dashboard link is public
