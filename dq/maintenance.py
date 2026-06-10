"""
Manutencao das tabelas Iceberg de streaming.

Roda OPTIMIZE (compacta data files e mescla delete files) e expurga snapshots
antigos / arquivos orfaos. Sem isso, o streaming acumula muitos arquivos e
snapshots, deixando as consultas (Trino/dashboard) cada vez mais lentas.

Executado periodicamente pela DAG `iceberg_maintenance` do Airflow.
"""

from __future__ import annotations

import os
import sys

from trino.dbapi import connect

TABELAS = [
    "gold.nota_fiscal",
    "silver.cabecalho", "silver.itens", "silver.impostos", "silver.cliente",
    "bronze.cabecalho", "bronze.itens", "bronze.impostos", "bronze.cliente",
    "lz.cabecalho", "lz.itens", "lz.impostos", "lz.cliente",
]


def main() -> int:
    conn = connect(
        host=os.getenv("TRINO_HOST", "trino"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user="maintenance",
        catalog="iceberg",
    )
    cur = conn.cursor()
    print("==> Manutencao Iceberg iniciando...")
    for t in TABELAS:
        for stmt in (
            f"ALTER TABLE iceberg.{t} EXECUTE optimize",
            f"ALTER TABLE iceberg.{t} EXECUTE expire_snapshots(retention_threshold => '5m')",
            f"ALTER TABLE iceberg.{t} EXECUTE remove_orphan_files(retention_threshold => '5m')",
        ):
            try:
                cur.execute(stmt)
                cur.fetchall()
                print(f"[OK]  {t} :: {stmt.split('EXECUTE ')[1]}")
            except Exception as exc:  # noqa: BLE001
                print(f"[skip] {t} :: {stmt.split('EXECUTE ')[1]} -> {str(exc)[:80]}")
    cur.close()
    conn.close()
    print("==> Manutencao concluida.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
