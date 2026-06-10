#!/usr/bin/env bash
# Inicializa o Airflow (migracao + usuario admin) e sobe scheduler + webserver.
set -euo pipefail

echo "==> Migrando banco de metadados do Airflow..."
airflow db migrate

echo "==> Garantindo usuario admin..."
airflow users create \
  --username "${_AIRFLOW_ADMIN_USER:-admin}" \
  --password "${_AIRFLOW_ADMIN_PASSWORD:-admin}" \
  --firstname Admin --lastname User --role Admin \
  --email admin@example.com || true

echo "==> Subindo scheduler (background) e webserver (foreground)..."
airflow scheduler &
exec airflow webserver
