"""
Gerador de eventos CDC (Change Data Capture).

Simula um banco Postgres emitindo alteracoes em tempo real para quatro
tabelas de um sistema de notas fiscais:

    - venda_cabecalho  (cabecalho da nota)
    - venda_itens      (itens da nota)
    - venda_impostos   (impostos da nota / item)
    - cliente          (cadastro de clientes)

Cada alteracao vira um arquivo JSON "achatado" (flat) na pasta de landing,
organizado por tabela:

    /data/landing/venda_cabecalho/000000000123_<uuid>.json

O envelope de cada evento contem:
    op            -> "I" (insert) ou "U" (update de algo ja enviado)
    source_table  -> nome da tabela de origem
    lsn           -> numero sequencial monotonico (simula o LSN do WAL)
    ingested_at   -> timestamp ISO da geracao
    <colunas...>  -> as colunas de negocio, no mesmo nivel do envelope

O fato de o JSON ser "flat" e a chave primaria estar no topo permite que o
producer use a PK como chave da mensagem Kafka, habilitando o upsert no Flink.

Importante: parte dos eventos sao ALTERACOES de registros ja emitidos
(mesma PK, op="U", lsn maior). E isso que exige a "inteligencia" de upsert /
deduplicacao nas camadas seguintes (silver/gold).
"""

from __future__ import annotations

import json
import os
import random
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from faker import Faker

fake = Faker("pt_BR")

# Tabelas / eventos suportados
TABELAS = ["venda_cabecalho", "venda_itens", "venda_impostos", "cliente"]

# Chave primaria de cada tabela (usada pelo producer como chave Kafka)
CHAVES_PRIMARIAS = {
    "venda_cabecalho": "nota_id",
    "venda_itens": "item_id",
    "venda_impostos": "imposto_id",
    "cliente": "cliente_id",
}

SEGMENTOS = ["Varejo", "Atacado", "Industria", "Servicos", "E-commerce"]
STATUS_NOTA = ["AUTORIZADA", "EMITIDA", "CANCELADA"]
TIPOS_IMPOSTO = ["ICMS", "IPI", "PIS", "COFINS"]
UNIDADES = ["UN", "CX", "KG", "LT", "PC"]


# ---------------------------------------------------------------------------
# Estado em memoria (permite gerar ALTERACOES coerentes de registros enviados)
# ---------------------------------------------------------------------------
class Estado:
    def __init__(self) -> None:
        self.lsn = 0
        self.clientes: dict[str, dict] = {}
        self.cabecalhos: dict[str, dict] = {}
        self.itens: dict[str, dict] = {}
        self.impostos: dict[str, dict] = {}
        self.seq = {"cliente": 0, "nota": 0, "item": 0, "imposto": 0}

    def proximo_lsn(self) -> int:
        self.lsn += 1
        return self.lsn

    def proximo_id(self, dominio: str, prefixo: str) -> str:
        self.seq[dominio] += 1
        return f"{prefixo}-{self.seq[dominio]:06d}"


def agora_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def envelopar(estado: Estado, tabela: str, op: str, registro: dict) -> dict:
    """Adiciona os metadados de CDC ao registro de negocio."""
    return {
        "op": op,
        "source_table": tabela,
        "lsn": estado.proximo_lsn(),
        "ingested_at": agora_iso(),
        **registro,
    }


# ---------------------------------------------------------------------------
# Construcao de registros novos (op = "I")
# ---------------------------------------------------------------------------
def novo_cliente(estado: Estado) -> dict:
    cliente_id = estado.proximo_id("cliente", "CLI")
    pessoa_juridica = random.random() < 0.5
    registro = {
        "cliente_id": cliente_id,
        "nome": fake.company() if pessoa_juridica else fake.name(),
        "documento": fake.cnpj() if pessoa_juridica else fake.cpf(),
        "tipo_pessoa": "J" if pessoa_juridica else "F",
        "email": fake.company_email() if pessoa_juridica else fake.email(),
        "uf": fake.estado_sigla(),
        "cidade": fake.city(),
        "segmento": random.choice(SEGMENTOS),
    }
    estado.clientes[cliente_id] = registro
    return registro


def novo_cabecalho(estado: Estado, cliente_id: str) -> dict:
    nota_id = estado.proximo_id("nota", "NF")
    registro = {
        "nota_id": nota_id,
        "numero_nota": str(random.randint(1000, 99999)),
        "serie": str(random.randint(1, 5)),
        "modelo": "55",
        "data_emissao": agora_iso(),
        "cliente_id": cliente_id,
        "natureza_operacao": random.choice(
            ["VENDA", "DEVOLUCAO", "REMESSA", "TRANSFERENCIA"]
        ),
        "valor_total": 0.0,  # consolidado pelos itens depois
        "status": "AUTORIZADA",
    }
    estado.cabecalhos[nota_id] = registro
    return registro


def novo_item(estado: Estado, nota_id: str, numero_item: int) -> dict:
    item_id = estado.proximo_id("item", "ITM")
    quantidade = round(random.uniform(1, 20), 3)
    valor_unitario = round(random.uniform(5, 500), 2)
    valor_desconto = round(valor_unitario * quantidade * random.uniform(0, 0.1), 2)
    valor_total = round(valor_unitario * quantidade - valor_desconto, 2)
    registro = {
        "item_id": item_id,
        "nota_id": nota_id,
        "numero_item": numero_item,
        "produto_id": f"PRD-{random.randint(1, 200):05d}",
        "produto_descricao": fake.bs().title(),
        "ncm": str(random.randint(10000000, 99999999)),
        "cfop": random.choice(["5102", "6102", "5405", "6108"]),
        "quantidade": quantidade,
        "unidade": random.choice(UNIDADES),
        "valor_unitario": valor_unitario,
        "valor_desconto": valor_desconto,
        "valor_total": valor_total,
    }
    estado.itens[item_id] = registro
    return registro


def novo_imposto(estado: Estado, nota_id: str, item_id: str, base: float) -> dict:
    imposto_id = estado.proximo_id("imposto", "IMP")
    tipo = random.choice(TIPOS_IMPOSTO)
    aliquota = round(random.choice([0.18, 0.12, 0.07, 0.0165, 0.076]), 4)
    valor = round(base * aliquota, 2)
    registro = {
        "imposto_id": imposto_id,
        "nota_id": nota_id,
        "item_id": item_id,
        "tipo_imposto": tipo,
        "cst": random.choice(["00", "20", "40", "60"]),
        "base_calculo": round(base, 2),
        "aliquota": aliquota,
        "valor": valor,
    }
    estado.impostos[imposto_id] = registro
    return registro


# ---------------------------------------------------------------------------
# Construcao de ALTERACOES de registros existentes (op = "U")
# ---------------------------------------------------------------------------
def alterar_cabecalho(estado: Estado) -> dict | None:
    if not estado.cabecalhos:
        return None
    registro = random.choice(list(estado.cabecalhos.values()))
    registro["status"] = random.choice(STATUS_NOTA)
    registro["valor_total"] = round(
        sum(
            i["valor_total"]
            for i in estado.itens.values()
            if i["nota_id"] == registro["nota_id"]
        ),
        2,
    )
    return registro


def alterar_item(estado: Estado) -> dict | None:
    if not estado.itens:
        return None
    registro = random.choice(list(estado.itens.values()))
    registro["quantidade"] = round(registro["quantidade"] * random.uniform(0.5, 1.5), 3)
    registro["valor_total"] = round(
        registro["valor_unitario"] * registro["quantidade"] - registro["valor_desconto"],
        2,
    )
    return registro


def alterar_cliente(estado: Estado) -> dict | None:
    if not estado.clientes:
        return None
    registro = random.choice(list(estado.clientes.values()))
    registro["segmento"] = random.choice(SEGMENTOS)
    registro["email"] = fake.email()
    return registro


# ---------------------------------------------------------------------------
# Fila de eventos pendentes (uma venda nova gera varios eventos no tempo)
# ---------------------------------------------------------------------------
def gerar_venda_completa(estado: Estado) -> deque:
    """Cria uma venda inteira e devolve a fila de eventos (op='I') a emitir."""
    fila: deque = deque()

    # Reaproveita um cliente existente as vezes; senao cria um novo
    if estado.clientes and random.random() < 0.6:
        cliente_id = random.choice(list(estado.clientes.keys()))
    else:
        cliente = novo_cliente(estado)
        cliente_id = cliente["cliente_id"]
        fila.append(("cliente", "I", dict(cliente)))

    cabecalho = novo_cabecalho(estado, cliente_id)
    nota_id = cabecalho["nota_id"]
    fila.append(("venda_cabecalho", "I", dict(cabecalho)))

    qtd_itens = random.randint(1, 4)
    for numero_item in range(1, qtd_itens + 1):
        item = novo_item(estado, nota_id, numero_item)
        fila.append(("venda_itens", "I", dict(item)))
        imposto = novo_imposto(estado, nota_id, item["item_id"], item["valor_total"])
        fila.append(("venda_impostos", "I", dict(imposto)))

    # Atualiza o valor_total do cabecalho de acordo com os itens
    cabecalho["valor_total"] = round(
        sum(e[2]["valor_total"] for e in fila if e[0] == "venda_itens"), 2
    )

    return fila


def gerar_alteracao(estado: Estado):
    """Sorteia uma alteracao de registro ja existente (op='U').

    So considera entidades que ja possuem registros emitidos, evitando
    escolher uma tabela vazia (que nao teria o que alterar).
    """
    opcoes = []
    if estado.cabecalhos:
        opcoes.append(("venda_cabecalho", alterar_cabecalho))
    if estado.itens:
        opcoes.append(("venda_itens", alterar_item))
    if estado.clientes:
        opcoes.append(("cliente", alterar_cliente))

    if not opcoes:
        return None

    tabela, func = random.choice(opcoes)
    registro = func(estado)
    if registro is None:
        return None
    return (tabela, "U", dict(registro))


# ---------------------------------------------------------------------------
# Escrita atomica do arquivo de landing
# ---------------------------------------------------------------------------
def escrever_evento(landing_dir: Path, estado: Estado, tabela: str, op: str, registro: dict) -> Path:
    evento = envelopar(estado, tabela, op, registro)
    pasta = landing_dir / tabela
    pasta.mkdir(parents=True, exist_ok=True)

    nome = f"{evento['lsn']:012d}_{uuid.uuid4().hex}.json"
    destino = pasta / nome
    tmp = pasta / (nome + ".tmp")

    # Escreve em .tmp e renomeia -> o producer nunca le um arquivo parcial
    tmp.write_text(json.dumps(evento, ensure_ascii=False), encoding="utf-8")
    tmp.rename(destino)
    return destino


# ---------------------------------------------------------------------------
# Loop principal
# ---------------------------------------------------------------------------
def main() -> None:
    landing_dir = Path(os.getenv("LANDING_DIR", "/data/landing"))
    min_rps = int(os.getenv("CDC_MIN_RPS", "1"))
    max_rps = int(os.getenv("CDC_MAX_RPS", "3"))
    update_ratio = float(os.getenv("CDC_UPDATE_RATIO", "0.35"))

    estado = Estado()
    pendentes: deque = deque()

    print(
        f"[cdc-generator] landing={landing_dir} rps={min_rps}-{max_rps} "
        f"update_ratio={update_ratio}",
        flush=True,
    )

    while True:
        # Quantos registros emitir neste segundo
        n = random.randint(min_rps, max_rps)

        for _ in range(n):
            # Decide entre uma ALTERACAO de algo existente ou um evento novo
            if random.random() < update_ratio:
                alteracao = gerar_alteracao(estado)
                if alteracao is not None:
                    pendentes.append(alteracao)

            # Garante que sempre exista evento novo na fila
            if not pendentes:
                pendentes.extend(gerar_venda_completa(estado))

            tabela, op, registro = pendentes.popleft()
            caminho = escrever_evento(landing_dir, estado, tabela, op, registro)
            print(f"[cdc-generator] {op} {tabela:<16} -> {caminho.name}", flush=True)

        time.sleep(1)


if __name__ == "__main__":
    main()
