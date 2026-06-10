# Contribuindo

Obrigado por contribuir! Este projeto é uma plataforma local de streaming CDC.

## Ambiente

- Docker Desktop (Compose v2) com **VM ≥ 10 GB** (ver [`docs/runbook.md`](docs/runbook.md)).
- Python 3.11 para os testes/lint.

```bash
pip install ruff pytest -r generator/requirements.txt -r producer/requirements.txt
ruff check .
pytest
```

## Subir a plataforma

```powershell
./scripts/run.ps1 up      # builda e sobe tudo
./scripts/run.ps1 jobs    # submete os jobs Flink
```

## Padrões

- **Lint**: `ruff check .` deve passar (config em `pyproject.toml`).
- **Testes**: `pytest` (gerador e producer têm testes unitários em `tests/`).
- **Fim de linha**: scripts `.sh`/`.py` são **LF** (ver `.gitattributes`) — não
  altere para CRLF, senão quebram dentro dos containers Linux.
- **Commits**: mensagens descritivas; uma feature/correção por commit.

## CI/CD

- `.github/workflows/ci.yml` roda lint, testes, validação de config e build das
  imagens leves a cada push/PR. Pode rodar localmente com
  [`act`](https://github.com/nektos/act): `act -j python-quality`.
- `.github/workflows/release.yml` publica um GitHub Release ao dar push de uma
  tag `vX.Y.Z`.

## Estrutura

Veja [`README.md`](README.md) e [`docs/arquitetura.md`](docs/arquitetura.md).
