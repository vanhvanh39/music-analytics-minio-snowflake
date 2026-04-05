from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "vietanh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="itunes_minio_snowflake_pipeline",
    default_args=default_args,
    description="MinIO -> Snowflake -> dbt pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["music", "data-engineering"],
) as dag:

    create_bucket = BashOperator(
        task_id="create_minio_bucket",
        bash_command="""
        cd /opt/airflow/project &&
        python scripts/create_minio_buckets.py
        """,
    )

    ingest_pipeline = BashOperator(
        task_id="run_ingest_pipeline",
        bash_command="""
        cd /opt/airflow/project &&
        python run_pipeline.py
        """,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
        cd /opt/airflow/project/dbt_music_analytics &&
        dbt run --profiles-dir .
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/project/dbt_music_analytics &&
        dbt test --profiles-dir .
        """,
    )

    create_bucket >> ingest_pipeline >> dbt_run >> dbt_test