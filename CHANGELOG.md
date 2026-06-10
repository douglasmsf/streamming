# Changelog

Todas as mudanças relevantes deste projeto são documentadas aqui.
O formato segue [Keep a Changelog](https://keepachangelog.com/pt-BR/1.1.0/)
e o projeto adota [Versionamento Semântico](https://semver.org/lang/pt-BR/).

## [Unreleased]

### Adicionado
- Dashboard/console em **tempo real consumindo o Kafka diretamente** (estado em
  memória, atualiza a cada mensagem; gráficos Chart.js de UF/segmento).
- DAG `iceberg_maintenance` (Airflow): `OPTIMIZE` + `expire_snapshots` para
  manter as tabelas Iceberg compactas e as consultas rápidas.
- Catálogo Iceberg REST com **backend Postgres** (`iceberg-db`), suportando
  commits concorrentes do Flink (substitui o SQLite, que travava com `SQLITE_BUSY`).
- `LICENSE` (MIT), `CONTRIBUTING.md`, `CHANGELOG.md` e `.env.example`.

### Alterado
- Trino com limites de memória/heap e `min-retention` para manutenção agressiva.

## [0.1.0] - 2026-06-10

### Adicionado
- Simulação de CDC (1–3 reg/s, inserts + updates) → landing → producer com DLQ.
- Kafka com tópicos por camada (`issuance_*_lz/bronze/silver`, `issuance_nota_gold`).
- Flink SQL: **Transformation** (lz→bronze→silver→gold) e **Persistence**
  (cada tópico → Iceberg/Parquet no MinIO).
- Trino para consulta; **dbt** (orquestrado pelo Airflow) para a camada `semantic`.
- **Great Expectations** para Data Quality; **REST API** com OAuth2 (JWT).
- CI/CD com GitHub Actions (lint/test/build + release por tag); documentação e
  diagrama da arquitetura.

[Unreleased]: https://github.com/douglasmsf/streamming/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/douglasmsf/streamming/releases/tag/v0.1.0
