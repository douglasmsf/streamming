# 🔑 Guia de acessos

Todos os endereços são **`http`** (não `https`) e locais (`localhost`).
As credenciais são **defaults de demonstração** (definidas no `.env` e no
`docker-compose.yml`) — troque em qualquer uso real.

## 🖥️ Interfaces web (abrir no navegador)

| Recurso | URL | Usuário | Senha | Para quê |
|---|---|---|---|---|
| **Console (tempo real)** | http://localhost:8050 | — | — | Dashboard ao vivo (KPIs, camadas, jobs, gráficos) |
| **REST API (serving)** | http://localhost:8060 | client `potencial` | secret `secret` | API analítica com OAuth2/JWT |
| **Flink** | http://localhost:8081 | — | — | Web UI dos jobs de streaming |
| **Trino** | http://localhost:8080 | qualquer (ex.: `admin`) | — | SQL ad-hoc sobre o Iceberg |
| **Airflow** | http://localhost:8082 | `admin` | `admin` | Orquestração (dbt, DQ, manutenção) |
| **Kafka UI** | http://localhost:8088 | — | — | Tópicos, mensagens, consumer groups |
| **MinIO (console)** | http://localhost:9001 | `admin` | `password` | Navegar os arquivos Parquet/Iceberg |

## 🔌 Endpoints de conexão (apps / clientes)

| Recurso | Endereço (host) | Endereço (entre containers) | Credenciais |
|---|---|---|---|
| Kafka (broker) | `localhost:29092` | `kafka:9092` | — (PLAINTEXT) |
| MinIO (S3 API) | http://localhost:9000 | `http://minio:9000` | `admin` / `password` |
| Iceberg REST (catálogo) | http://localhost:8181 | `http://iceberg-rest:8181` | — |
| Postgres (Airflow) | — | `airflow-db:5432` (db `airflow`) | `airflow` / `airflow` |
| Postgres (catálogo Iceberg) | — | `iceberg-db:5432` (db `iceberg`) | `iceberg` / `iceberg` |

## 🧪 Exemplos de uso

### Trino (CLI)
```bash
docker compose exec trino trino
# depois:  SELECT * FROM iceberg.gold.nota_fiscal LIMIT 20;
```

### REST API com OAuth2 (client credentials → JWT)
```bash
# 1) obter token
TOKEN=$(curl -s -XPOST http://localhost:8060/oauth/token \
  -d client_id=potencial -d client_secret=secret | jq -r .access_token)

# 2) consumir endpoint protegido
curl -s http://localhost:8060/api/v1/faturamento/uf \
  -H "Authorization: Bearer $TOKEN"
```
Endpoints: `/api/v1/faturamento/uf`, `/api/v1/faturamento/cliente`,
`/api/v1/produtos/ranking`, `/api/v1/impostos`, `/api/v1/vendas/diarias`.

### Kafka (listar tópicos)
```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

## 🔄 Mapa de portas

| Porta (host) | Serviço |
|---|---|
| 8050 | Dashboard / Console |
| 8060 | REST API (serving) |
| 8080 | Trino |
| 8081 | Flink Web UI |
| 8082 | Airflow |
| 8088 | Kafka UI |
| 8181 | Iceberg REST |
| 9000 | MinIO (S3 API) |
| 9001 | MinIO (console) |
| 29092 | Kafka (acesso pelo host) |

> Para alterar credenciais/portas: edite o `.env` (MinIO/Airflow/CDC) e as
> variáveis de ambiente no `docker-compose.yml` (REST API, Postgres).
