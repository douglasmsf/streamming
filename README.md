# Plataforma de Streaming CDC — Notas Fiscais

Plataforma de dados **100% local** que simula um **CDC (Change Data Capture)**
de um Postgres de notas fiscais e o processa em tempo real numa arquitetura
**medallion** (bronze → silver → gold), com persistência em **Apache Iceberg /
Parquet**, consulta via **Trino** e camada analítica em **dbt** orquestrado pelo
**Airflow**.

É o equivalente **local** de uma arquitetura AWS de referência (DMS → MSK →
Flink → S3/Iceberg → Glue DQ → REST API).

## Desenho técnico

![Arquitetura](docs/arquitetura.svg)

> Versão visual: abra [`docs/arquitetura.svg`](docs/arquitetura.svg) no
> navegador. Detalhes em [`docs/arquitetura.md`](docs/arquitetura.md).

```
INGESTAO (DMS-like + DLQ)        KAFKA (MSK) por camada        FLINK
cdc-generator -> /data/landing   issuance_*_lz                 Transformation:
   -> folder-producer (DLQ) ---> issuance_*_bronze   <=======   lz->bronze->silver->gold
                                 issuance_*_silver             Persistence:
                                 issuance_nota_gold  ========>   cada topico -> Iceberg
                                                               (MinIO/Parquet)
ICEBERG (lz/bronze/silver/gold/semantic)  ->  Trino  ->  dbt(Semantic) + Great Expectations(DQ)
                                                      ->  REST API (oAuth2) + Dashboard/Console
```

### Mapeamento AWS → local

| Referência AWS | Equivalente local |
|---|---|
| DMS (fullload & CDC) | `cdc-generator` + `folder-producer` → landing |
| Lambda + SQS FIFO (DLQ) | producer com retry/DLQ (`/data/dlq`) |
| MSK | Kafka (KRaft), tópicos por camada |
| Flink Transformation (topic→topic) | `flink/sql/transform.sql` |
| Flink Persistence (committers) | `flink/sql/persist.sql` |
| S3 Landing/Bronze/Silver/Gold/Semantic | MinIO + Iceberg (schemas `lz/bronze/silver/gold/semantic`) |
| Glue DQ (~3h) | Great Expectations (Airflow) |
| Semantic / REST API / IDP oAuth2 | dbt (semantic) + REST API Flask (JWT) |

## Stack

| Camada | Tecnologia |
|--------|-----------|
| Geração de CDC | Python (gerador + producer pasta→Kafka) |
| Mensageria | Apache Kafka (KRaft) — 1 tópico por evento |
| Processamento streaming | Apache Flink (SQL) |
| Tabelas / storage | Apache Iceberg + Parquet sobre MinIO (S3) |
| Catálogo | Iceberg REST |
| Consulta | Trino |
| Orquestração / Analytics | Airflow + dbt (dbt-trino) |
| CI/CD | GitHub Actions (executável localmente com `act`) |

## Início rápido

```powershell
# Windows (PowerShell)
./scripts/run.ps1 up        # builda e sobe tudo
./scripts/run.ps1 jobs      # submete os jobs Flink (bronze/silver/gold)
./scripts/run.ps1 trino     # abre o CLI do Trino
```

```bash
# Linux / macOS / WSL
make up
make jobs
make trino
```

Depois (no Trino):
```sql
SELECT * FROM iceberg.gold.gold_nota_fiscal ORDER BY atualizado_em DESC LIMIT 20;
```

Passo a passo completo em [`docs/runbook.md`](docs/runbook.md).

## O que este projeto demonstra

- **Simulação de CDC em tempo real**: 1 a 3 registros/segundo, misturando
  inserts e **alterações de registros já enviados** (mesma PK, `op="U"`).
- **Ingestão pasta → Kafka** com a **PK como chave** da mensagem (habilita
  upsert).
- **Medallion com Flink SQL**: bronze (histórico append), silver (upsert +
  deduplicação + limpeza/renome de colunas), gold (join consolidado em tempo
  real).
- **Iceberg upsert** (`format-version=2`) que converge para o estado atual a
  cada alteração — ver [`docs/cdc-e-upsert.md`](docs/cdc-e-upsert.md).
- **Trino** para consulta ad-hoc e **dbt + Airflow** para os marts analíticos.
- **Dashboard em tempo real** (http://localhost:8050) com KPIs, faturamento por
  UF/segmento e as últimas notas chegando via upsert.
- **CI/CD** com GitHub Actions, rodável offline com `act`.

## Interfaces web

| UI | URL | Credenciais |
|----|-----|-------------|
| **Console (tudo num lugar)** | **http://localhost:8050** | — |
| REST API (serving + oAuth2) | http://localhost:8060 | client `potencial` / `secret` |
| Flink | http://localhost:8081 | — |
| Trino | http://localhost:8080 | — |
| Airflow | http://localhost:8082 | admin / admin |
| Kafka UI | http://localhost:8088 | — |
| MinIO | http://localhost:9001 | admin / password |

> O **Console** (http://localhost:8050) mostra numa página só: status de cada
> serviço, registros por camada (lz→bronze→silver→gold→semantic) em tempo real,
> os jobs Flink rodando, KPIs/dados de venda ao vivo e o diagrama da arquitetura.

## Estrutura do repositório

```
.
├── docker-compose.yml         # toda a plataforma
├── generator/                 # gerador de eventos CDC -> /data/landing
├── producer/                  # le a pasta e publica no Kafka
├── flink/                     # Dockerfile (jars) + sql/ (bronze, silver, gold)
├── trino/                     # catalogo Iceberg + queries de exemplo
├── dbt/                       # projeto dbt (marts analiticos) sobre Trino
├── airflow/                   # imagem + DAG que orquestra o dbt
├── scripts/run.ps1            # atalhos para Windows
├── tests/                     # testes unitarios (gerador/producer)
├── docs/                      # arquitetura, modelo de dados, runbook, CDC/upsert
└── .github/workflows/         # CI (lint/test/build) e CD (release por tag)
```

## Documentação

- [Arquitetura e diagrama](docs/arquitetura.md)
- [Modelo de dados](docs/modelo-dados.md)
- [CDC, updates e upsert](docs/cdc-e-upsert.md)
- [Runbook (operação)](docs/runbook.md)

## Desenvolvimento

```bash
pip install ruff pytest -r generator/requirements.txt -r producer/requirements.txt
ruff check .
pytest
```
