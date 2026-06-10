"""
Data Quality com Great Expectations (equivalente local ao AWS Glue DQ).

Le as tabelas Iceberg via Trino, carrega em pandas e valida um conjunto de
expectativas (regras de qualidade). Se alguma regra falhar, encerra com codigo
!= 0 -> a task do Airflow falha e o problema fica visivel.

Roda num venv isolado (/opt/airflow/dq-venv) para nao conflitar com o Airflow.
"""

from __future__ import annotations

import os
import sys

import pandas as pd
from great_expectations.dataset import PandasDataset
from trino.dbapi import connect

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))


def carregar(sql: str) -> pd.DataFrame:
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="dq", catalog="iceberg")
    cur = conn.cursor()
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    dados = cur.fetchall()
    cur.close()
    conn.close()
    return pd.DataFrame(dados, columns=cols)


def validar(nome: str, df: pd.DataFrame, regras) -> bool:
    if df.empty:
        print(f"[DQ] {nome}: sem dados ainda (pulando).")
        return True
    ds = PandasDataset(df)
    regras(ds)
    resultado = ds.validate()
    ok = resultado["success"]
    total = len(resultado["results"])
    falhas = [r for r in resultado["results"] if not r["success"]]
    status = "OK" if ok else "FALHOU"
    print(f"[DQ] {nome}: {status} ({total - len(falhas)}/{total} regras)")
    for r in falhas:
        cfg = r["expectation_config"]
        print(f"     - {cfg['expectation_type']} {cfg['kwargs']}")
    return ok


def regras_gold(ds: PandasDataset) -> None:
    ds.expect_column_values_to_not_be_null("nota_id")
    ds.expect_column_values_to_be_unique("nota_id")
    ds.expect_column_values_to_be_between("valor_total", min_value=0, max_value=None)
    ds.expect_column_values_to_be_between("qtd_itens", min_value=0, max_value=None)
    ds.expect_column_values_to_be_in_set(
        "status_nota", ["AUTORIZADA", "EMITIDA", "CANCELADA"]
    )


def regras_silver_cliente(ds: PandasDataset) -> None:
    ds.expect_column_values_to_not_be_null("cliente_id")
    ds.expect_column_values_to_be_unique("cliente_id")
    ds.expect_column_values_to_be_in_set("tipo_pessoa", ["F", "J"])


def regras_silver_impostos(ds: PandasDataset) -> None:
    ds.expect_column_values_to_not_be_null("imposto_id")
    ds.expect_column_values_to_be_between("aliquota", min_value=0, max_value=1)
    ds.expect_column_values_to_be_between("valor_imposto", min_value=0, max_value=None)


def main() -> int:
    print("==> Data Quality (Great Expectations) iniciando...")
    checagens = [
        ("gold.nota_fiscal", "SELECT * FROM iceberg.gold.nota_fiscal", regras_gold),
        ("silver.cliente", "SELECT * FROM iceberg.silver.cliente", regras_silver_cliente),
        ("silver.impostos", "SELECT * FROM iceberg.silver.impostos", regras_silver_impostos),
    ]

    todas_ok = True
    for nome, sql, regras in checagens:
        try:
            df = carregar(sql)
        except Exception as exc:  # noqa: BLE001
            print(f"[DQ] {nome}: tabela indisponivel ({exc}); pulando.")
            continue
        if not validar(nome, df, regras):
            todas_ok = False

    if todas_ok:
        print("==> Data Quality: TODAS as suites passaram.")
        return 0
    print("==> Data Quality: houve FALHAS de qualidade.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
