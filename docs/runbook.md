# Runbook — operação local

## Pré-requisitos
- Docker Desktop (com Docker Compose v2)
- **Memória do VM do Docker ≥ 10 GB** (são vários JVMs: Kafka, Flink JM+TM,
  Trino, Airflow, 2× Postgres). No Windows/WSL2 ajuste em `%USERPROFILE%\.wslconfig`:
  ```ini
  [wsl2]
  memory=11GB
  processors=6
  swap=4GB
  ```
  Depois `wsl --shutdown` e reabra o Docker Desktop. Com menos memória o Trino/Flink
  podem ser mortos por OOM.
- Windows: use `scripts/run.ps1`. Linux/macOS/WSL: use o `Makefile`.

## 1. Subir a infraestrutura

```powershell
./scripts/run.ps1 up        # Windows
# make up                   # Linux/macOS
```

Isso builda as imagens (Flink/Airflow baixam jars/pacotes na 1ª vez) e sobe
todos os serviços. Aguarde ~1-2 min até o Kafka/MinIO/Flink ficarem prontos.

URLs:
- Flink UI: http://localhost:8081
- Trino: http://localhost:8080
- Airflow: http://localhost:8082 (admin / admin)
- Kafka UI: http://localhost:8088
- MinIO: http://localhost:9001 (admin / password)

## 2. Submeter os jobs Flink (bronze → silver → gold)

```powershell
./scripts/run.ps1 jobs      # Windows
# make jobs                 # Linux/macOS
```

Confira em http://localhost:8081 que 3 jobs ficam **RUNNING**
(`bronze-ingestion`, `silver-upsert`, `gold-nota-fiscal`).

> Os dados aparecem no Iceberg a cada checkpoint (~30s).

## 3. Consultar no Trino

```powershell
./scripts/run.ps1 trino
```
```sql
SELECT * FROM iceberg.gold.gold_nota_fiscal ORDER BY atualizado_em DESC LIMIT 20;
```
Mais exemplos em [`../trino/sql/exploracao.sql`](../trino/sql/exploracao.sql).

## 4. Rodar o dbt (camada analítica) via Airflow

A DAG `dbt_gold_analytics` roda a cada 10 min. Para disparar na hora:

```powershell
./scripts/run.ps1 dag
```
Ou pela UI do Airflow (http://localhost:8082). Depois consulte:
```sql
SELECT * FROM iceberg.analytics.mart_faturamento_por_uf ORDER BY faturamento DESC;
```

## 5. Inspecionar o "CDC bruto"
Os arquivos JSON gerados ficam em `./data/landing/<tabela>/` (antes de irem
para o Kafka) e em `./data/processed/<tabela>/` (depois de publicados).

## Operações úteis

```powershell
./scripts/run.ps1 ps        # status dos serviços
./scripts/run.ps1 logs      # logs ao vivo
./scripts/run.ps1 down      # derruba (mantém dados)
./scripts/run.ps1 clean     # derruba + apaga volumes e dados locais
```

## Troubleshooting

| Sintoma | Causa provável / solução |
|--------|--------------------------|
| Jobs Flink falham com `ClassNotFound hadoop` | A imagem do Flink precisa ter buildado os jars. Rode `docker compose build flink-jobmanager`. |
| Trino não enxerga as tabelas | Os jobs Flink ainda não fizeram o 1º checkpoint (~60s) ou não foram submetidos. |
| Persistência Flink em `RESTARTING` com `SQLITE_BUSY` | Já resolvido: o catálogo usa Postgres (`iceberg-db`), não SQLite. Confirme que `iceberg-db` está `healthy`. |
| Jobs não aparecem no cluster (submit) | O `submit_jobs.sh` escreve `execution.target: remote` no `flink-conf.yaml`; confira que o JobManager subiu com `rest.bind-address: 0.0.0.0`. |
| Persistência falha com `Unable to load region` | Os serviços Flink precisam de `AWS_REGION` (já no compose). |
| Trino reiniciando / OOM | Aumente a memória do VM do Docker (ver pré-requisitos). Trino está capado em 3 GB (`mem_limit`). |
| `dbt` falha em criar o schema | O Trino precisa estar no ar e o catálogo `iceberg` acessível. Veja `dbt debug` na DAG. |
| Nada chega no Kafka | Veja os logs de `cdc-generator` e `folder-producer`; confira tópicos no Kafka UI. |
| Porta em uso | Ajuste o mapeamento de portas no `docker-compose.yml`. |

## CI/CD

- **CI** (`.github/workflows/ci.yml`): lint (ruff), testes (pytest), validação
  de configuração e build das imagens leves. Rode localmente com
  [`act`](https://github.com/nektos/act): `act -j python-quality`.
- **CD** (`.github/workflows/release.yml`): ao dar push de uma tag `vX.Y.Z`,
  publica um GitHub Release.
