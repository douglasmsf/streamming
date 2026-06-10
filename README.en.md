# 🛰️ CDC Streaming Platform — Real-Time Lakehouse

[🇧🇷 Português](README.md) | **🇬🇧 English**

> **End-to-end, fully-local data pipeline:** simulated CDC → **Kafka** → **Flink** (medallion) → **Iceberg/Parquet** → **Trino** + **dbt** + **Airflow**, with a **real-time dashboard** and a **REST API**.
>
> *The local equivalent of an AWS lakehouse (DMS → MSK → Flink → S3/Iceberg → Glue DQ → REST API).*

[![CI](https://github.com/douglasmsf/streamming/actions/workflows/ci.yml/badge.svg)](https://github.com/douglasmsf/streamming/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/tag/douglasmsf/streamming?label=release&sort=semver)](https://github.com/douglasmsf/streamming/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
![Docker](https://img.shields.io/badge/Docker%20Compose-local-2496ED?logo=docker&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-KRaft-231F20?logo=apachekafka)
![Flink](https://img.shields.io/badge/Apache%20Flink-SQL-E6526F?logo=apacheflink)
![Iceberg](https://img.shields.io/badge/Apache%20Iceberg-Parquet-1A73E8)
![Trino](https://img.shields.io/badge/Trino-SQL-DD00A1?logo=trino)
![dbt](https://img.shields.io/badge/dbt-semantic-FF694B?logo=dbt)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-orchestration-017CEE?logo=apacheairflow)

---

## 🗺️ Architecture

![Architecture](docs/arquitetura.svg)

```
INGESTION (DMS-like + DLQ)        KAFKA (MSK) per layer         FLINK SQL
cdc-generator → /data/landing     issuance_*_lz                 Transformation:
   → folder-producer (DLQ) ───►   issuance_*_bronze   ◄══════    lz→bronze→silver→gold
                                  issuance_*_silver             Persistence:
                                  issuance_nota_gold  ══════►     each topic → Iceberg

ICEBERG (lz/bronze/silver/gold/semantic) on MinIO (S3)
   → Trino (SQL) → dbt (semantic layer) + Great Expectations (Data Quality)
   → REST API (OAuth2/JWT) + real-time Dashboard/Console (reads from Kafka)
```

## ⭐ Highlights

- 🔄 **Real-time CDC** — 1–3 events/sec mixing *inserts* and **updates to records already sent** (same PK, `op="U"`).
- 🧱 **Medallion on Flink SQL** — *Transformation* (lz→bronze→silver→gold **between Kafka topics**) and *Persistence* (each topic → **Iceberg/Parquet**), mirroring a real AWS architecture.
- 🧊 **Apache Iceberg (upsert)** — `format-version 2` + *equality deletes*: silver/gold converge to the current state on every change (dedup by key).
- 📊 **Real-time dashboard** — consumes **Kafka directly** (in-memory state, updates on every message); live KPIs, per-state/segment charts, and latest invoices.
- 🧪 **Data Quality** with Great Expectations and a **semantic layer** with dbt (orchestrated by Airflow).
- 🔌 **REST API** with OAuth2 (client credentials → JWT) serving the analytics layer.
- 🛠️ **Resilience** — producer with **DLQ/retry** (simulating Lambda+SQS FIFO), Iceberg catalog on Postgres, automatic maintenance (`OPTIMIZE`/`expire_snapshots`).
- ⚙️ **CI/CD** with GitHub Actions (lint, tests, build, release on tag) — runnable offline with `act`.

## 🧰 Stack

| Layer | Technology |
|--------|-----------|
| CDC generation | Python (generator + folder→Kafka producer, with DLQ) |
| Messaging | Apache Kafka (KRaft) — per-layer topics |
| Stream processing | Apache Flink (SQL) |
| Tables / storage | Apache Iceberg + Parquet on MinIO (S3) |
| Catalog | Iceberg REST (Postgres backend) |
| Query (batch/ad-hoc) | Trino |
| Analytics / Semantic | dbt (dbt-trino) |
| Data Quality | Great Expectations |
| Orchestration | Apache Airflow |
| Serving | REST API (Flask) + OAuth2/JWT |
| Observability | Real-time Dashboard/Console (Flask + Kafka) |
| CI/CD | GitHub Actions (runnable with `act`) |

## 🚀 Getting started

> Requirement: **Docker Desktop** with a **VM ≥ 10 GB** (see [`docs/runbook.md`](docs/runbook.md)).

```powershell
# Windows (PowerShell)
./scripts/run.ps1 up        # build and start the whole platform
./scripts/run.ps1 jobs      # submit the Flink jobs (transform + persist)
```

```bash
# Linux / macOS / WSL
make up
make jobs
```

Then open the **real-time Console** 👉 **http://localhost:8050**

## 🖥️ Interfaces

| UI | URL | Credentials |
|----|-----|-------------|
| **Console (real-time)** | **http://localhost:8050** | — |
| REST API (serving) | http://localhost:8060 | client `potencial` / `secret` |
| Flink | http://localhost:8081 | — |
| Trino | http://localhost:8080 | — |
| Airflow | http://localhost:8082 | admin / admin |
| Kafka UI | http://localhost:8088 | — |
| MinIO | http://localhost:9001 | admin / password |

> 📋 Full access guide (URLs, endpoints, credentials and examples) in
> [`docs/acessos.md`](docs/acessos.md).

## 💡 What this project demonstrates

- **Streaming data engineering**: CDC, messaging, declarative stream processing (Flink SQL).
- **Lakehouse / Iceberg**: medallion, key-based *upsert*/dedup, *time travel*, compaction and *snapshot expiry*.
- **Modeling & Analytics**: semantic layer with dbt, tests and data contracts.
- **Reliability**: DLQ/retry, automated Data Quality, table maintenance.
- **Platform**: orchestration (Airflow), authenticated serving, real-time observability.
- **Software engineering**: IaC with Docker Compose, tests, lint and CI/CD.
- **Cloud→local translation**: faithful mapping of an **AWS** architecture (DMS, MSK, Glue DQ, S3) to local open-source components.

## 🗂️ Repository structure

```
.
├── docker-compose.yml         # the whole platform (14 services)
├── generator/                 # CDC event generator -> /data/landing
├── producer/                  # folder -> Kafka (with DLQ/retry)
├── flink/                     # Dockerfile (jars) + sql/ (transform.sql, persist.sql)
├── trino/                     # config + Iceberg catalog + sample queries
├── dbt/                       # dbt project (semantic layer) on Trino
├── dq/                        # Great Expectations + Iceberg maintenance
├── airflow/                   # image + DAGs (dbt, DQ, maintenance)
├── serving/                   # REST API (Flask) + OAuth2
├── dashboard/                 # real-time console (Flask + Kafka)
├── scripts/run.ps1            # shortcuts (Windows)
├── tests/                     # unit tests (generator/producer)
├── docs/                      # architecture (+SVG), data model, CDC/upsert, runbook, access
└── .github/workflows/         # CI (lint/test/build) + CD (release on tag)
```

## 📚 Documentation

- [🗺️ Architecture & diagram](docs/arquitetura.md)
- [🧬 Data model](docs/modelo-dados.md)
- [🔄 CDC, updates and upsert](docs/cdc-e-upsert.md)
- [⚙️ Runbook (operations & troubleshooting)](docs/runbook.md)
- [🔑 Access guide (URLs, credentials & examples)](docs/acessos.md)

> Docs are written in Portuguese — open an issue if you'd like English versions.

## 🧪 Development

```bash
pip install ruff pytest -r generator/requirements.txt -r producer/requirements.txt
ruff check .
pytest
```

## 📄 License

[MIT](LICENSE) — feel free to use, study and adapt.
