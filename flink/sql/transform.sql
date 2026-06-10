-- ==========================================================================
-- FLINK "TRANSFORMATION"  (Consumer/Producer: move dados ENTRE os topicos)
-- --------------------------------------------------------------------------
-- Espelha o bloco "Transformation" da arquitetura: jobs que consomem um
-- topico, transformam e produzem no proximo topico, dentro do Kafka:
--
--   issuance_*_lz  --(padroniza)-->  issuance_*_bronze
--   issuance_*_bronze --(dedup/upsert/limpa/renomeia)--> issuance_*_silver
--   issuance_*_silver --(join)--> issuance_nota_gold
--
-- Nao toca no Iceberg: isso e responsabilidade do job de Persistence.
-- ==========================================================================

SET 'pipeline.name' = 'transformation';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

-- ======================= LANDING ZONE (append sources) ====================
CREATE TEMPORARY TABLE lz_cabecalho (
  op STRING, source_table STRING, lsn BIGINT, ingested_at STRING,
  nota_id STRING, numero_nota STRING, serie STRING, modelo STRING,
  data_emissao STRING, cliente_id STRING, natureza_operacao STRING,
  valor_total DOUBLE, status STRING
) WITH ('connector'='kafka','topic'='issuance_cabecalho_lz',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-lz-cabecalho',
  'scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');

CREATE TEMPORARY TABLE lz_itens (
  op STRING, source_table STRING, lsn BIGINT, ingested_at STRING,
  item_id STRING, nota_id STRING, numero_item INT, produto_id STRING,
  produto_descricao STRING, ncm STRING, cfop STRING, quantidade DOUBLE,
  unidade STRING, valor_unitario DOUBLE, valor_desconto DOUBLE, valor_total DOUBLE
) WITH ('connector'='kafka','topic'='issuance_itens_lz',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-lz-itens',
  'scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');

CREATE TEMPORARY TABLE lz_impostos (
  op STRING, source_table STRING, lsn BIGINT, ingested_at STRING,
  imposto_id STRING, nota_id STRING, item_id STRING, tipo_imposto STRING,
  cst STRING, base_calculo DOUBLE, aliquota DOUBLE, valor DOUBLE
) WITH ('connector'='kafka','topic'='issuance_impostos_lz',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-lz-impostos',
  'scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');

CREATE TEMPORARY TABLE lz_cliente (
  op STRING, source_table STRING, lsn BIGINT, ingested_at STRING,
  cliente_id STRING, nome STRING, documento STRING, tipo_pessoa STRING,
  email STRING, uf STRING, cidade STRING, segmento STRING
) WITH ('connector'='kafka','topic'='issuance_cliente_lz',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-lz-cliente',
  'scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');

-- ======================= BRONZE (append sink, keyed por PK) ===============
CREATE TEMPORARY TABLE bronze_cabecalho_sink (
  nota_id STRING, op STRING, lsn BIGINT, ingested_at STRING, numero_nota STRING,
  serie STRING, modelo STRING, data_emissao STRING, cliente_id STRING,
  natureza_operacao STRING, valor_total DOUBLE, status STRING
) WITH ('connector'='kafka','topic'='issuance_cabecalho_bronze',
  'properties.bootstrap.servers'='kafka:9092','key.format'='raw','key.fields'='nota_id',
  'value.format'='json');

CREATE TEMPORARY TABLE bronze_itens_sink (
  item_id STRING, op STRING, lsn BIGINT, ingested_at STRING, nota_id STRING,
  numero_item INT, produto_id STRING, produto_descricao STRING, ncm STRING,
  cfop STRING, quantidade DOUBLE, unidade STRING, valor_unitario DOUBLE,
  valor_desconto DOUBLE, valor_total DOUBLE
) WITH ('connector'='kafka','topic'='issuance_itens_bronze',
  'properties.bootstrap.servers'='kafka:9092','key.format'='raw','key.fields'='item_id',
  'value.format'='json');

CREATE TEMPORARY TABLE bronze_impostos_sink (
  imposto_id STRING, op STRING, lsn BIGINT, ingested_at STRING, nota_id STRING,
  item_id STRING, tipo_imposto STRING, cst STRING, base_calculo DOUBLE,
  aliquota DOUBLE, valor DOUBLE
) WITH ('connector'='kafka','topic'='issuance_impostos_bronze',
  'properties.bootstrap.servers'='kafka:9092','key.format'='raw','key.fields'='imposto_id',
  'value.format'='json');

CREATE TEMPORARY TABLE bronze_cliente_sink (
  cliente_id STRING, op STRING, lsn BIGINT, ingested_at STRING, nome STRING,
  documento STRING, tipo_pessoa STRING, email STRING, uf STRING, cidade STRING,
  segmento STRING
) WITH ('connector'='kafka','topic'='issuance_cliente_bronze',
  'properties.bootstrap.servers'='kafka:9092','key.format'='raw','key.fields'='cliente_id',
  'value.format'='json');

-- ======================= BRONZE (upsert source p/ silver) =================
CREATE TEMPORARY TABLE bronze_cabecalho_src (
  nota_id STRING, op STRING, lsn BIGINT, ingested_at STRING, numero_nota STRING,
  serie STRING, modelo STRING, data_emissao STRING, cliente_id STRING,
  natureza_operacao STRING, valor_total DOUBLE, status STRING,
  PRIMARY KEY (nota_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_cabecalho_bronze',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-br-cabecalho',
  'key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

CREATE TEMPORARY TABLE bronze_itens_src (
  item_id STRING, op STRING, lsn BIGINT, ingested_at STRING, nota_id STRING,
  numero_item INT, produto_id STRING, produto_descricao STRING, ncm STRING,
  cfop STRING, quantidade DOUBLE, unidade STRING, valor_unitario DOUBLE,
  valor_desconto DOUBLE, valor_total DOUBLE,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_itens_bronze',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-br-itens',
  'key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

CREATE TEMPORARY TABLE bronze_impostos_src (
  imposto_id STRING, op STRING, lsn BIGINT, ingested_at STRING, nota_id STRING,
  item_id STRING, tipo_imposto STRING, cst STRING, base_calculo DOUBLE,
  aliquota DOUBLE, valor DOUBLE,
  PRIMARY KEY (imposto_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_impostos_bronze',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-br-impostos',
  'key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

CREATE TEMPORARY TABLE bronze_cliente_src (
  cliente_id STRING, op STRING, lsn BIGINT, ingested_at STRING, nome STRING,
  documento STRING, tipo_pessoa STRING, email STRING, uf STRING, cidade STRING,
  segmento STRING,
  PRIMARY KEY (cliente_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_cliente_bronze',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-br-cliente',
  'key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

-- ======================= SILVER (upsert sink + source) ====================
CREATE TEMPORARY TABLE silver_cabecalho_sink (
  nota_id STRING, numero_nota STRING, serie STRING, modelo STRING,
  data_emissao STRING, data_emissao_dia DATE, cliente_id STRING,
  natureza_operacao STRING, valor_total_nota DECIMAL(15,2), status_nota STRING,
  atualizado_em STRING, PRIMARY KEY (nota_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_cabecalho_silver',
  'properties.bootstrap.servers'='kafka:9092','key.format'='raw','value.format'='json');

CREATE TEMPORARY TABLE silver_itens_sink (
  item_id STRING, nota_id STRING, numero_item INT, produto_id STRING,
  descricao_produto STRING, ncm STRING, cfop STRING, quantidade DECIMAL(15,3),
  unidade STRING, valor_unitario DECIMAL(15,2), valor_desconto DECIMAL(15,2),
  valor_bruto DECIMAL(15,2), valor_liquido DECIMAL(15,2), atualizado_em STRING,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_itens_silver',
  'properties.bootstrap.servers'='kafka:9092','key.format'='raw','value.format'='json');

CREATE TEMPORARY TABLE silver_impostos_sink (
  imposto_id STRING, nota_id STRING, item_id STRING, tipo_imposto STRING,
  cst STRING, base_calculo DECIMAL(15,2), aliquota DECIMAL(7,4),
  valor_imposto DECIMAL(15,2), atualizado_em STRING,
  PRIMARY KEY (imposto_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_impostos_silver',
  'properties.bootstrap.servers'='kafka:9092','key.format'='raw','value.format'='json');

CREATE TEMPORARY TABLE silver_cliente_sink (
  cliente_id STRING, nome_cliente STRING, documento STRING, tipo_pessoa STRING,
  uf STRING, municipio STRING, segmento STRING, atualizado_em STRING,
  PRIMARY KEY (cliente_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_cliente_silver',
  'properties.bootstrap.servers'='kafka:9092','key.format'='raw','value.format'='json');

CREATE TEMPORARY TABLE silver_cabecalho_src (
  nota_id STRING, numero_nota STRING, serie STRING, data_emissao_dia DATE,
  cliente_id STRING, natureza_operacao STRING, valor_total_nota DECIMAL(15,2),
  status_nota STRING, atualizado_em STRING, PRIMARY KEY (nota_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_cabecalho_silver',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-sv-cabecalho',
  'key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

CREATE TEMPORARY TABLE silver_itens_src (
  item_id STRING, nota_id STRING, quantidade DECIMAL(15,3),
  valor_desconto DECIMAL(15,2), valor_liquido DECIMAL(15,2),
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_itens_silver',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-sv-itens',
  'key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

CREATE TEMPORARY TABLE silver_impostos_src (
  imposto_id STRING, nota_id STRING, valor_imposto DECIMAL(15,2),
  PRIMARY KEY (imposto_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_impostos_silver',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-sv-impostos',
  'key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

CREATE TEMPORARY TABLE silver_cliente_src (
  cliente_id STRING, nome_cliente STRING, uf STRING, segmento STRING,
  PRIMARY KEY (cliente_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_cliente_silver',
  'properties.bootstrap.servers'='kafka:9092','properties.group.id'='tf-sv-cliente',
  'key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

-- ======================= GOLD (upsert sink) ===============================
CREATE TEMPORARY TABLE gold_sink (
  nota_id STRING, numero_nota STRING, serie STRING, data_emissao_dia DATE,
  status_nota STRING, natureza_operacao STRING, cliente_id STRING,
  nome_cliente STRING, uf_cliente STRING, segmento_cliente STRING,
  qtd_itens INT, qtd_produtos DECIMAL(15,3), valor_produtos DECIMAL(15,2),
  valor_descontos DECIMAL(15,2), valor_impostos DECIMAL(15,2),
  valor_total DECIMAL(15,2), atualizado_em STRING,
  PRIMARY KEY (nota_id) NOT ENFORCED
) WITH ('connector'='upsert-kafka','topic'='issuance_nota_gold',
  'properties.bootstrap.servers'='kafka:9092','key.format'='raw','value.format'='json');

-- views de agregacao p/ a gold
CREATE TEMPORARY VIEW agg_itens AS
SELECT nota_id, CAST(COUNT(*) AS INT) AS qtd_itens,
       CAST(SUM(quantidade) AS DECIMAL(15,3)) AS qtd_produtos,
       CAST(SUM(valor_liquido) AS DECIMAL(15,2)) AS valor_produtos,
       CAST(SUM(valor_desconto) AS DECIMAL(15,2)) AS valor_descontos
FROM silver_itens_src GROUP BY nota_id;

CREATE TEMPORARY VIEW agg_impostos AS
SELECT nota_id, CAST(SUM(valor_imposto) AS DECIMAL(15,2)) AS valor_impostos
FROM silver_impostos_src GROUP BY nota_id;

-- ======================= JOB 1: lz -> bronze (padronizacao) ===============
BEGIN STATEMENT SET;
INSERT INTO bronze_cabecalho_sink
SELECT nota_id, UPPER(op), lsn, ingested_at, numero_nota, serie, modelo,
       data_emissao, cliente_id, natureza_operacao, valor_total, status FROM lz_cabecalho;
INSERT INTO bronze_itens_sink
SELECT item_id, UPPER(op), lsn, ingested_at, nota_id, numero_item, produto_id,
       produto_descricao, ncm, cfop, quantidade, unidade, valor_unitario,
       valor_desconto, valor_total FROM lz_itens;
INSERT INTO bronze_impostos_sink
SELECT imposto_id, UPPER(op), lsn, ingested_at, nota_id, item_id, tipo_imposto,
       cst, base_calculo, aliquota, valor FROM lz_impostos;
INSERT INTO bronze_cliente_sink
SELECT cliente_id, UPPER(op), lsn, ingested_at, nome, documento, tipo_pessoa,
       email, uf, cidade, segmento FROM lz_cliente;
END;

-- ======================= JOB 2: bronze -> silver (dedup/limpa/renomeia) ===
BEGIN STATEMENT SET;
INSERT INTO silver_cabecalho_sink
SELECT nota_id, numero_nota, serie, modelo, data_emissao,
       CAST(SUBSTRING(data_emissao FROM 1 FOR 10) AS DATE), cliente_id,
       natureza_operacao, CAST(valor_total AS DECIMAL(15,2)), status, ingested_at
FROM bronze_cabecalho_src;
INSERT INTO silver_itens_sink
SELECT item_id, nota_id, numero_item, produto_id, produto_descricao, ncm, cfop,
       CAST(quantidade AS DECIMAL(15,3)), unidade,
       CAST(valor_unitario AS DECIMAL(15,2)), CAST(valor_desconto AS DECIMAL(15,2)),
       CAST(valor_unitario * quantidade AS DECIMAL(15,2)),
       CAST(valor_total AS DECIMAL(15,2)), ingested_at
FROM bronze_itens_src;
INSERT INTO silver_impostos_sink
SELECT imposto_id, nota_id, item_id, tipo_imposto, cst,
       CAST(base_calculo AS DECIMAL(15,2)), CAST(aliquota AS DECIMAL(7,4)),
       CAST(valor AS DECIMAL(15,2)), ingested_at
FROM bronze_impostos_src;
INSERT INTO silver_cliente_sink
SELECT cliente_id, nome, documento, tipo_pessoa, uf, cidade, segmento, ingested_at
FROM bronze_cliente_src;
END;

-- ======================= JOB 3: silver -> gold (join) =====================
INSERT INTO gold_sink
SELECT
  c.nota_id, c.numero_nota, c.serie, c.data_emissao_dia, c.status_nota,
  c.natureza_operacao, c.cliente_id, cl.nome_cliente, cl.uf, cl.segmento,
  COALESCE(i.qtd_itens, 0),
  COALESCE(i.qtd_produtos, CAST(0 AS DECIMAL(15,3))),
  COALESCE(i.valor_produtos, CAST(0 AS DECIMAL(15,2))),
  COALESCE(i.valor_descontos, CAST(0 AS DECIMAL(15,2))),
  COALESCE(t.valor_impostos, CAST(0 AS DECIMAL(15,2))),
  COALESCE(i.valor_produtos, CAST(0 AS DECIMAL(15,2)))
    + COALESCE(t.valor_impostos, CAST(0 AS DECIMAL(15,2))),
  c.atualizado_em
FROM silver_cabecalho_src c
LEFT JOIN agg_itens i    ON c.nota_id = i.nota_id
LEFT JOIN agg_impostos t ON c.nota_id = t.nota_id
LEFT JOIN silver_cliente_src cl ON c.cliente_id = cl.cliente_id;
