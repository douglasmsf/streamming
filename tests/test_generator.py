import json

import cdc_generator as gen


def test_venda_completa_gera_eventos_coerentes():
    estado = gen.Estado()
    fila = gen.gerar_venda_completa(estado)

    tabelas = [e[0] for e in fila]
    # Toda venda tem ao menos 1 cabecalho, 1 item e 1 imposto
    assert "venda_cabecalho" in tabelas
    assert "venda_itens" in tabelas
    assert "venda_impostos" in tabelas

    # Todos os eventos novos sao inserts
    assert all(e[1] == "I" for e in fila)

    # O cabecalho referencia um cliente existente no estado
    cabecalho = next(e[2] for e in fila if e[0] == "venda_cabecalho")
    assert cabecalho["cliente_id"] in estado.clientes


def test_envelope_adiciona_metadados_cdc():
    estado = gen.Estado()
    registro = gen.novo_cliente(estado)
    evento = gen.envelopar(estado, "cliente", "I", registro)

    assert evento["op"] == "I"
    assert evento["source_table"] == "cliente"
    assert evento["lsn"] == 1
    assert "ingested_at" in evento
    assert evento["cliente_id"] == registro["cliente_id"]


def test_lsn_e_monotonico():
    estado = gen.Estado()
    assert estado.proximo_lsn() == 1
    assert estado.proximo_lsn() == 2
    assert estado.proximo_lsn() == 3


def test_alteracao_usa_mesma_pk_e_op_update():
    estado = gen.Estado()
    cliente = gen.novo_cliente(estado)
    alteracao = gen.gerar_alteracao(estado)

    assert alteracao is not None
    tabela, op, registro = alteracao
    assert op == "U"
    if tabela == "cliente":
        # Mesma PK do registro original -> habilita upsert
        assert registro["cliente_id"] == cliente["cliente_id"]


def test_escrever_evento_cria_arquivo_json(tmp_path):
    estado = gen.Estado()
    registro = gen.novo_cliente(estado)
    caminho = gen.escrever_evento(tmp_path, estado, "cliente", "I", registro)

    assert caminho.exists()
    assert caminho.suffix == ".json"
    dados = json.loads(caminho.read_text(encoding="utf-8"))
    assert dados["source_table"] == "cliente"
    assert dados["op"] == "I"
