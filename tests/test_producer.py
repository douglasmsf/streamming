import json

import folder_to_kafka as prod


class FakeProducer:
    """Captura produce() e simula entrega (on_delivery) com sucesso/falha."""

    def __init__(self, sucesso=True):
        self.mensagens = []
        self.sucesso = sucesso

    def produce(self, topic, key, value, on_delivery=None):
        self.mensagens.append({"topic": topic, "key": key, "value": value})
        if on_delivery:
            on_delivery(None if self.sucesso else "erro-simulado", None)

    def poll(self, _):
        pass

    def flush(self, _):
        pass


def test_entidades_cobrem_todas_as_tabelas():
    assert set(prod.ENTIDADES) == {
        "venda_cabecalho",
        "venda_itens",
        "venda_impostos",
        "cliente",
    }


def test_topico_lz_segue_o_padrao_issuance():
    assert prod.topico_lz("venda_cabecalho", "issuance") == "issuance_cabecalho_lz"
    assert prod.topico_lz("cliente", "issuance") == "issuance_cliente_lz"


def test_publicar_usa_chave_pk_e_topico_correto(tmp_path):
    pasta = tmp_path / "cliente"
    pasta.mkdir()
    evento = {"op": "I", "source_table": "cliente", "cliente_id": "CLI-000001"}
    arquivo = pasta / "000000000001_abc.json"
    arquivo.write_text(json.dumps(evento), encoding="utf-8")

    producer = FakeProducer(sucesso=True)
    resultado: dict[str, bool] = {}
    ok = prod.publicar(producer, arquivo, "cliente", "issuance", resultado)

    assert ok is True
    msg = producer.mensagens[0]
    assert msg["topic"] == "issuance_cliente_lz"
    assert msg["key"] == b"CLI-000001"          # chave = PK -> habilita upsert
    assert resultado[arquivo.name] is True       # entrega com sucesso


def test_mover_para_dlq_em_caso_de_falha(tmp_path):
    arquivo = tmp_path / "origem" / "000000000002_def.json"
    arquivo.parent.mkdir(parents=True)
    arquivo.write_text("{}", encoding="utf-8")

    dlq = tmp_path / "dlq"
    prod.mover(arquivo, dlq, "cliente")

    assert not arquivo.exists()
    assert (dlq / "cliente" / arquivo.name).exists()
