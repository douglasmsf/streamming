FROM apache/airflow:2.10.3-python3.11

# dbt (adaptador Trino) no ambiente do Airflow
RUN pip install --no-cache-dir \
    "dbt-core==1.8.7" \
    "dbt-trino==1.8.3"

# Great Expectations num venv ISOLADO (evita conflito de deps com o Airflow).
# A DAG de DQ chama /opt/airflow/dq-venv/bin/python.
RUN python -m venv /opt/airflow/dq-venv \
 && /opt/airflow/dq-venv/bin/pip install --no-cache-dir \
      "numpy==1.24.4" "pandas==1.5.3" "great-expectations==0.15.50" "trino==0.330.0"
