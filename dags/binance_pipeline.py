"""End-to-end Binance daily-candles pipeline: fetch -> GCS -> BigQuery -> dbt."""
import datetime as dt
import os
import subprocess

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.task_group import TaskGroup

RAW_DIR = "/tmp/binance_raw"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
           "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT"]

PROJECT = os.environ["GCP_PROJECT"]
DATASET = os.environ.get("BQ_DATASET", "binance_analytics")
BUCKET = os.environ["GCS_BUCKET"]

SCHEMA = [
    {"name": "symbol", "type": "STRING"},
    {"name": "open_time", "type": "TIMESTAMP"},
    {"name": "open", "type": "FLOAT64"},
    {"name": "high", "type": "FLOAT64"},
    {"name": "low", "type": "FLOAT64"},
    {"name": "close", "type": "FLOAT64"},
    {"name": "volume", "type": "FLOAT64"},
    {"name": "close_time", "type": "TIMESTAMP"},
    {"name": "quote_volume", "type": "FLOAT64"},
    {"name": "trades", "type": "INT64"},
    {"name": "open_date", "type": "DATE"},
]

DDL = f"""
CREATE TABLE IF NOT EXISTS `{PROJECT}.{DATASET}.raw_candles` (
  symbol STRING, open_time TIMESTAMP, open FLOAT64, high FLOAT64,
  low FLOAT64, close FLOAT64, volume FLOAT64, close_time TIMESTAMP,
  quote_volume FLOAT64, trades INT64, open_date DATE
)
PARTITION BY open_date
CLUSTER BY symbol;
"""


def fetch(ds, **kwargs):
    """Download candles for [ds, ds+1) for every symbol (one daily batch)."""
    sys_path = os.path.join(os.path.dirname(__file__), "..", "scripts")
    import sys
    sys.path.insert(0, os.path.abspath(sys_path))
    from ingest_binance import fetch_klines
    import pathlib
    start = dt.datetime.strptime(ds, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1)
    ms = lambda d: int(d.timestamp() * 1000)
    for symbol in SYMBOLS:
        try:
            df = fetch_klines(symbol, "1d", ms(start), ms(end))
        except Exception:
            import logging
            logging.getLogger(__name__).exception("fetch failed for %s", symbol)
            continue
        if df.empty:
            continue
        out = pathlib.Path(RAW_DIR) / f"symbol={symbol}"
        out.mkdir(parents=True, exist_ok=True)
        import pyarrow as pa
        import pyarrow.parquet as pq
        schema = pa.schema([
            ("symbol", pa.string()),
            ("open_time", pa.timestamp("us", tz="UTC")),
            ("open", pa.float64()), ("high", pa.float64()), ("low", pa.float64()),
            ("close", pa.float64()), ("volume", pa.float64()),
            ("close_time", pa.timestamp("us", tz="UTC")),
            ("quote_volume", pa.float64()),
            ("trades", pa.int64()),
            ("open_date", pa.date32()),
        ])
        pq.write_table(pa.Table.from_pandas(df, schema=schema), str(out / "data.parquet"))


default_args = {"retries": 2, "retry_delay": dt.timedelta(minutes=1)}

with DAG(
    dag_id="binance_daily_pipeline",
    description="Fetch Binance daily candles -> GCS -> BigQuery -> dbt",
    schedule_interval="@daily",
    start_date=dt.datetime(2025, 1, 1),
    catchup=True,
    default_args=default_args,
    max_active_runs=1,
    tags=["binance", "de-zoomcamp"],
) as dag:

    fetch_task = PythonOperator(task_id="fetch_binance_data", python_callable=fetch)

    create_table = BigQueryExecuteQueryOperator(
        task_id="ensure_raw_table", sql=DDL, use_legacy_sql=False)

    with TaskGroup("upload_to_lake") as upload_group:
        for symbol in SYMBOLS:
            LocalFilesystemToGCSOperator(
                task_id=f"upload_{symbol.lower()}",
                src=f"{RAW_DIR}/symbol={symbol}/data.parquet",
                dst=f"raw/candles/symbol={symbol}/date={{{{ ds }}}}/data.parquet",
                bucket=BUCKET,
            )

    with TaskGroup("load_to_warehouse") as load_group:
        for symbol in SYMBOLS:
            GCSToBigQueryOperator(
                task_id=f"load_{symbol.lower()}",
                bucket=BUCKET,
                source_objects=[f"raw/candles/symbol={symbol}/date={{{{ ds }}}}/*.parquet"],
                destination_project_dataset_table=f"{PROJECT}.{DATASET}.raw_candles",
                schema_fields=SCHEMA,
                source_format="PARQUET",
                write_disposition="WRITE_APPEND",
                time_partitioning=None,  # partitioning defined by table DDL
            )

    dbt_run = BashOperator(
        task_id="dbt_transform",
        bash_command="cd /opt/dbt/binance && dbt run --profiles-dir .",
    )

    fetch_task >> create_table >> upload_group >> load_group >> dbt_run
