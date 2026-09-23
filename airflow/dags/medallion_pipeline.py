"""
DAG principal do Apache Airflow para o pipeline medallion.

Orquestra as camadas do lakehouse (Bronze -> Silver -> Gold),
executando os jobs PySpark existentes via BashOperator:

    bronze_ingest >> silver_process >> gold_aggregate

Cada task roda `python -m src...` com cwd=/opt/project (volume do compose),
onde ficam data/, .env e o metastore Derby (metastore_minio/).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/project"

DEFAULT_ARGS = {
    "owner": "medallion_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="medallion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline Medallion: Bronze -> Silver -> Gold (PySpark + Delta + MinIO)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["medallion", "lakehouse", "spark", "delta"],
) as dag:

    bronze_ingest = BashOperator(
        task_id="bronze_ingest",
        bash_command="python -m src.ingestion.Bronze",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=30),
    )

    silver_process = BashOperator(
        task_id="silver_process",
        bash_command="python -m src.processing.Silver",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=30),
    )

    gold_aggregate = BashOperator(
        task_id="gold_aggregate",
        bash_command="python -m src.serving.Gold",
        cwd=PROJECT_DIR,
        execution_timeout=timedelta(minutes=30),
    )

    bronze_ingest >> silver_process >> gold_aggregate
