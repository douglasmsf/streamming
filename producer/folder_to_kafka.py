"""
Producer "pasta -> Kafka" com DLQ/retry (simula Lambda + SQS FIFO).

Observa a pasta de landing (onde o gerador CDC / "DMS" escreve os arquivos
JSON) e publica cada evento no topico de LANDING ZONE da entidade:

    /data/landing/venda_cabecalho/*.json -> topico issuance_cabecalho_lz
    /data/landing/venda_itens/*.json      -> topico issuance_itens_lz
    /data/landing/venda_impostos/*.json   -> topico issuance_impostos_lz
    /data/landing/cliente/*.json          -> topico issuance_cliente_lz

A partir desse "lz" o Flink (Transformation) move lz -> bronze -> silver ->
gold dentro do Kafka, e o Flink (Persistence) commita cada camada no Iceberg.

Resiliencia (equivalente Lambda+SQS FIFO/DLQ):
  * a CHAVE da mensagem e a PK -> ordenacao por entidade + upsert.
  * arquivos lidos em ordem de LSN (FIFO).
  * sucesso  -> move para /data/processed.
  * falha    -> move para /data/dlq e e reprocessado (retry com FIFO).
"""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

from confluent_kafka import Producer

# tabela de origem -> (nome curto p/ topico, campo de PK)
ENTIDADES = {
    "venda_cabecalho": ("cabecalho", "nota_id"),
    "venda_itens": ("itens", "item_id"),
    "venda_impostos": ("impostos", "imposto_id"),
    "cliente": ("cliente", "cliente_id"),
}


def topico_lz(tabela: str, prefix: str) -> str:
    curto, _ = ENTIDADES[tabela]
    return f"{prefix}_{curto}_lz"


def criar_producer(bootstrap: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "client.id": "folder-producer",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "message.send.max.retries": 5,
        }
    )


def publicar(producer: Producer, arquivo: Path, tabela: str, prefix: str, resultado: dict) -> bool:
    """Le um arquivo e o publica. Marca sucesso/falha em `resultado`."""
    try:
        evento = json.loads(arquivo.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, FileNotFoundError):
        return False  # arquivo ainda sendo escrito; tenta depois

    _, campo_pk = ENTIDADES[tabela]
    chave = str(evento.get(campo_pk, ""))

    def entrega(err, _msg, _nome=arquivo.name):
        resultado[_nome] = err is None

    producer.produce(
        topic=topico_lz(tabela, prefix),
        key=chave.encode("utf-8"),
        value=json.dumps(evento, ensure_ascii=False).encode("utf-8"),
        on_delivery=entrega,
    )
    producer.poll(0)
    return True


def mover(arquivo: Path, base: Path, tabela: str) -> None:
    destino = base / tabela
    destino.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(arquivo), str(destino / arquivo.name))
    except FileNotFoundError:
        pass


def varrer(pasta_base: Path, tabela: str):
    pasta = pasta_base / tabela
    if not pasta.exists():
        return []
    return sorted(p for p in pasta.glob("*.json") if not p.name.endswith(".tmp"))


def main() -> None:
    landing_dir = Path(os.getenv("LANDING_DIR", "/data/landing"))
    processed_dir = Path(os.getenv("PROCESSED_DIR", "/data/processed"))
    dlq_dir = Path(os.getenv("DLQ_DIR", "/data/dlq"))
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    prefix = os.getenv("TOPIC_PREFIX", "issuance")

    landing_dir.mkdir(parents=True, exist_ok=True)
    producer = criar_producer(bootstrap)

    print(
        f"[producer] landing={landing_dir} dlq={dlq_dir} bootstrap={bootstrap} prefix={prefix}",
        flush=True,
    )

    while True:
        resultado: dict[str, bool] = {}
        lidos: list[tuple[Path, str]] = []

        for tabela in ENTIDADES:
            # 1) reprocessa a DLQ primeiro (retry FIFO, como SQS FIFO)
            for arquivo in varrer(dlq_dir, tabela):
                if publicar(producer, arquivo, tabela, prefix, resultado):
                    lidos.append((arquivo, tabela))
            # 2) novos arquivos da landing
            for arquivo in varrer(landing_dir, tabela):
                if publicar(producer, arquivo, tabela, prefix, resultado):
                    lidos.append((arquivo, tabela))

        if lidos:
            producer.flush(10)
            ok = err = 0
            for arquivo, tabela in lidos:
                if resultado.get(arquivo.name, False):
                    mover(arquivo, processed_dir, tabela)
                    ok += 1
                else:
                    mover(arquivo, dlq_dir, tabela)  # vai pra DLQ e tenta de novo
                    err += 1
            msg = f"[producer] publicados {ok}"
            if err:
                msg += f" | {err} -> DLQ (retry)"
            print(msg, flush=True)

        time.sleep(0.3)


if __name__ == "__main__":
    main()
