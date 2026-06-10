"""
DAG do Airflow que roda o Data Quality (Great Expectations).

Equivalente local ao AWS Glue DQ do desenho: valida regras de qualidade nas
tabelas Iceberg (gold/silver) via Trino. Roda num venv isolado para nao
conflitar com as dependencias do Airflow.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

with DAG(
    dag_id="dq_great_expectations",
    description="Data Quality (Great Expectations) sobre as tabelas Iceberg",
    default_args={"owner": "dados", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",  # equivalente ao Glue DQ (~3h em prod)
    catchup=False,
    max_active_runs=1,
    tags=["data-quality", "great-expectations"],
) as dag:
    dq = BashOperator(
        task_id="great_expectations",
        bash_command="/opt/airflow/dq-venv/bin/python /opt/airflow/dq/expectations.py",
        env={"TRINO_HOST": "trino", "TRINO_PORT": "8080"},
        append_env=True,
    )
