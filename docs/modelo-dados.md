# Modelo de dados

Quatro entidades de um sistema de notas fiscais. Cada evento CDC é um JSON
*flat* (envelope + colunas de negócio no mesmo nível).

## Envelope CDC (comum a todos os eventos)

| Campo         | Tipo   | Descrição |
|---------------|--------|-----------|
| `op`          | string | `I` (insert) ou `U` (update de algo já enviado) |
| `source_table`| string | Tabela de origem |
| `lsn`         | long   | Sequencial monotônico (simula o LSN do WAL) |
| `ingested_at` | string | Timestamp ISO da geração |

A **chave primária** de cada tabela vai no topo do JSON e é usada como **chave
da mensagem Kafka** (habilita o upsert).

## Tabelas de origem (bronze – nomes originais)

### `venda_cabecalho` — PK `nota_id`
`numero_nota`, `serie`, `modelo`, `data_emissao`, `cliente_id`,
`natureza_operacao`, `valor_total`, `status`

### `venda_itens` — PK `item_id`
`nota_id`, `numero_item`, `produto_id`, `produto_descricao`, `ncm`, `cfop`,
`quantidade`, `unidade`, `valor_unitario`, `valor_desconto`, `valor_total`

### `venda_impostos` — PK `imposto_id`
`nota_id`, `item_id`, `tipo_imposto` (ICMS/IPI/PIS/COFINS), `cst`,
`base_calculo`, `aliquota`, `valor`

### `cliente` — PK `cliente_id`
`nome`, `documento`, `tipo_pessoa`, `email`, `uf`, `cidade`, `segmento`

## Silver (limpa, deduplicada, colunas renomeadas)

| Tabela | PK | Principais transformações |
|--------|----|--------|
| `silver.dim_cliente` | `cliente_id` | `nome`→`nome_cliente`, `cidade`→`municipio`, `ingested_at`→`atualizado_em` |
| `silver.fato_venda_cabecalho` | `nota_id` | `valor_total`→`valor_total_nota` (DECIMAL), `status`→`status_nota`, deriva `data_emissao_dia` (DATE) |
| `silver.fato_venda_item` | `item_id` | `produto_descricao`→`descricao_produto`, tipa DECIMAL, deriva `valor_bruto` e `valor_liquido` |
| `silver.fato_venda_imposto` | `imposto_id` | `valor`→`valor_imposto`, tipa DECIMAL |

## Gold (consolidada – uma linha por nota)

### `gold.gold_nota_fiscal` — PK `nota_id`
`numero_nota`, `serie`, `data_emissao_dia`, `status_nota`, `natureza_operacao`,
`cliente_id`, `nome_cliente`, `uf_cliente`, `segmento_cliente`,
`qtd_itens`, `qtd_produtos`, `valor_produtos`, `valor_descontos`,
`valor_impostos`, `valor_total`, `atualizado_em`

## Analytics (marts dbt em `iceberg.analytics`)

| Mart | Grão | Fonte |
|------|------|-------|
| `mart_faturamento_por_cliente` | cliente | gold |
| `mart_faturamento_por_uf` | UF | gold |
| `mart_vendas_diarias` | dia | gold |
| `mart_ranking_produtos` | produto | silver (itens) |
| `mart_resumo_impostos` | tipo de imposto | silver (impostos) |
