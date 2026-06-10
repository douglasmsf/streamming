"""
DAG do Airflow que orquestra o dbt para construir a camada SEMANTIC.

Os modelos dbt rodam sobre o Trino e materializam tabelas Iceberg no schema
`iceberg.semantic` a partir das camadas gold/silver (atualizadas em tempo
real pelo Flink).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

DBT_DIR = "/opt/airflow/dbt"
DBT_ENV = {"DBT_PROFILES_DIR": DBT_DIR, "DBT_PROJECT_DIR": DBT_DIR}

with DAG(
    dag_id="dbt_semantic",
    description="Constroi a camada semantic com dbt sobre o Trino",
    default_args={"owner": "dados", "retries": 1, "retry_delay": timedelta(minutes=2)},
    start_date=datetime(2024, 1, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "semantic"],
) as dag:
    run = BashOperator(task_id="dbt_run", bash_command=f"cd {DBT_DIR} && dbt run",
                       env=DBT_ENV, append_env=True)
    test = BashOperator(task_id="dbt_test", bash_command=f"cd {DBT_DIR} && dbt test",
                        env=DBT_ENV, append_env=True)
    run >> test
