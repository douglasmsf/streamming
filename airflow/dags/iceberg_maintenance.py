"""
DAG de manutencao das tabelas Iceberg (compactacao + expurgo).

Mantem as tabelas de streaming compactas para que as consultas (Trino e o
dashboard em tempo real) continuem rapidas mesmo apos horas de ingestao.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

with DAG(
    dag_id="iceberg_maintenance",
    description="OPTIMIZE + expire_snapshots nas tabelas Iceberg de streaming",
    default_args={"owner": "dados", "retries": 1, "retry_delay": timedelta(minutes=1)},
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",  # a cada 5 minutos
    catchup=False,
    max_active_runs=1,
    tags=["manutencao", "iceberg"],
) as dag:
    BashOperator(
        task_id="optimize_e_expire",
        bash_command="/opt/airflow/dq-venv/bin/python /opt/airflow/dq/maintenance.py",
        env={"TRINO_HOST": "trino", "TRINO_PORT": "8080"},
        append_env=True,
    )
