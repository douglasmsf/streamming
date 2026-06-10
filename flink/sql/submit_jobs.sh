#!/usr/bin/env bash
# ===========================================================================
# Submete os jobs Flink ao cluster, no modelo Transformation + Persistence:
#   transform.sql -> lz->bronze->silver->gold (entre topicos Kafka)
#   persist.sql   -> cada topico de camada -> Iceberg/MinIO
# Cada BEGIN STATEMENT SET vira um job de streaming continuo.
# ===========================================================================
set -euo pipefail

SQL_DIR="/opt/sql"
SQL_CLIENT="/opt/flink/bin/sql-client.sh"
CONF="/opt/flink/conf/flink-conf.yaml"

# O entrypoint custom nao aplica FLINK_PROPERTIES; configuramos o cluster
# remoto direto no flink-conf.yaml para o sql-client submeter ao JobManager.
{
  echo "execution.target: remote"
  echo "jobmanager.rpc.address: flink-jobmanager"
  echo "rest.address: flink-jobmanager"
  echo "rest.port: 8081"
} >> "${CONF}"

echo "==> Aguardando o JobManager do Flink (flink-jobmanager:8081)..."
for i in $(seq 1 60); do
  if curl -sf "http://flink-jobmanager:8081/overview" >/dev/null 2>&1; then
    echo "    JobManager disponivel."; break
  fi
  sleep 2
done

submit() {
  echo ""; echo "==> Submetendo $1 ..."
  "${SQL_CLIENT}" -f "${SQL_DIR}/$1"
  echo "    $1 submetido."
}

submit "transform.sql"
submit "persist.sql"

echo ""
echo "==> Jobs submetidos. Acompanhe em http://localhost:8081"
