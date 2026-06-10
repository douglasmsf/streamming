# Atalhos (Linux/macOS/WSL). No Windows use scripts/run.ps1.
.PHONY: up jobs trino dag ps logs down clean test lint

up:            ## Builda e sobe a infraestrutura
	docker compose up -d --build

jobs:          ## Submete os jobs Flink (bronze/silver/gold)
	docker compose run --rm flink-sql-submit

trino:         ## Abre o CLI do Trino
	docker compose exec trino trino

dag:           ## Dispara a DAG do dbt no Airflow
	docker compose exec airflow airflow dags trigger dbt_gold_analytics

ps:            ## Lista os servicos
	docker compose ps

logs:          ## Acompanha os logs
	docker compose logs -f --tail=100

down:          ## Derruba os containers
	docker compose down

clean:         ## Derruba containers + volumes + dados locais
	docker compose down -v
	rm -rf data/landing data/processed

test:          ## Roda os testes unitarios (gerador/producer)
	pip install -r generator/requirements.txt pytest >/dev/null
	pytest -q

lint:          ## Lint Python
	pip install ruff >/dev/null
	ruff check .
