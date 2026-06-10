<#
.SYNOPSIS
  Atalhos para subir/operar a plataforma de streaming no Windows.

.EXAMPLE
  ./scripts/run.ps1 up       # builda e sobe toda a infraestrutura
  ./scripts/run.ps1 jobs     # submete os jobs Flink (bronze/silver/gold)
  ./scripts/run.ps1 trino    # abre o CLI do Trino
  ./scripts/run.ps1 dag      # dispara a DAG do dbt no Airflow
  ./scripts/run.ps1 down     # derruba tudo
#>
param(
  [Parameter(Position = 0)]
  [ValidateSet("up", "jobs", "trino", "dag", "ps", "logs", "down", "clean")]
  [string]$Command = "up"
)

$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

switch ($Command) {
  "up" {
    Write-Host "==> Buildando e subindo a infraestrutura..." -ForegroundColor Cyan
    docker compose up -d --build
    Write-Host "`n==> Servicos no ar. Aguarde ~1min e rode: ./scripts/run.ps1 jobs" -ForegroundColor Green
    Write-Host "    Dashboard: http://localhost:8050  (tempo real)"
    Write-Host "    Flink UI : http://localhost:8081"
    Write-Host "    Trino    : http://localhost:8080"
    Write-Host "    Airflow  : http://localhost:8082 (admin/admin)"
    Write-Host "    Kafka UI : http://localhost:8088"
    Write-Host "    MinIO    : http://localhost:9001 (admin/password)"
  }
  "jobs" {
    Write-Host "==> Submetendo jobs Flink (bronze/silver/gold)..." -ForegroundColor Cyan
    docker compose run --rm flink-sql-submit
  }
  "trino" {
    docker compose exec trino trino
  }
  "dag" {
    Write-Host "==> Disparando a DAG dbt_gold_analytics..." -ForegroundColor Cyan
    docker compose exec airflow airflow dags trigger dbt_gold_analytics
  }
  "ps" { docker compose ps }
  "logs" { docker compose logs -f --tail=100 }
  "down" {
    Write-Host "==> Derrubando containers..." -ForegroundColor Yellow
    docker compose down
  }
  "clean" {
    Write-Host "==> Derrubando containers + volumes + dados locais..." -ForegroundColor Red
    docker compose down -v
    Remove-Item -Recurse -Force ./data/landing, ./data/processed -ErrorAction SilentlyContinue
  }
}
