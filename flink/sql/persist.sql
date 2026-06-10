-- ==========================================================================
-- FLINK "PERSISTENCE"  (Consumer/Committers: cada topico -> Iceberg/MinIO)
-- --------------------------------------------------------------------------
-- Espelha o bloco "Persistence" da arquitetura: jobs que consomem cada
-- topico de camada e COMMITAM em tabelas Iceberg (Parquet) no MinIO:
--
--   issuance_*_lz      -> iceberg.lz.*       (append, "Landing bucket")
--   issuance_*_bronze  -> iceberg.bronze.*   (append, "Bronze bucket")
--   issuance_*_silver  -> iceberg.silver.*   (upsert, "Silver bucket")
--   issuance_nota_gold -> iceberg.gold.*     (upsert, "Gold bucket")
-- ==========================================================================

SET 'pipeline.name' = 'persistence';
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG iceberg WITH (
  'type'='iceberg','catalog-impl'='org.apache.iceberg.rest.RESTCatalog',
  'uri'='http://iceberg-rest:8181','warehouse'='s3://warehouse/',
  'io-impl'='org.apache.iceberg.aws.s3.S3FileIO','s3.endpoint'='http://minio:9000',
  's3.path-style-access'='true','s3.access-key-id'='admin','s3.secret-access-key'='password');

CREATE DATABASE IF NOT EXISTS iceberg.lz;
CREATE DATABASE IF NOT EXISTS iceberg.bronze;
CREATE DATABASE IF NOT EXISTS iceberg.silver;
CREATE DATABASE IF NOT EXISTS iceberg.gold;

-- ============================ ICEBERG: LZ (append) ========================
CREATE TABLE IF NOT EXISTS iceberg.lz.cabecalho (
  op STRING, lsn BIGINT, ingested_at STRING, nota_id STRING, numero_nota STRING,
  serie STRING, modelo STRING, data_emissao STRING, cliente_id STRING,
  natureza_operacao STRING, valor_total DOUBLE, status STRING
) WITH ('format-version'='2');
CREATE TABLE IF NOT EXISTS iceberg.lz.itens (
  op STRING, lsn BIGINT, ingested_at STRING, item_id STRING, nota_id STRING,
  numero_item INT, produto_id STRING, produto_descricao STRING, ncm STRING,
  cfop STRING, quantidade DOUBLE, unidade STRING, valor_unitario DOUBLE,
  valor_desconto DOUBLE, valor_total DOUBLE
) WITH ('format-version'='2');
CREATE TABLE IF NOT EXISTS iceberg.lz.impostos (
  op STRING, lsn BIGINT, ingested_at STRING, imposto_id STRING, nota_id STRING,
  item_id STRING, tipo_imposto STRING, cst STRING, base_calculo DOUBLE,
  aliquota DOUBLE, valor DOUBLE
) WITH ('format-version'='2');
CREATE TABLE IF NOT EXISTS iceberg.lz.cliente (
  op STRING, lsn BIGINT, ingested_at STRING, cliente_id STRING, nome STRING,
  documento STRING, tipo_pessoa STRING, email STRING, uf STRING, cidade STRING,
  segmento STRING
) WITH ('format-version'='2');

-- ============================ ICEBERG: BRONZE (append) ====================
CREATE TABLE IF NOT EXISTS iceberg.bronze.cabecalho LIKE iceberg.lz.cabecalho;
CREATE TABLE IF NOT EXISTS iceberg.bronze.itens     LIKE iceberg.lz.itens;
CREATE TABLE IF NOT EXISTS iceberg.bronze.impostos  LIKE iceberg.lz.impostos;
CREATE TABLE IF NOT EXISTS iceberg.bronze.cliente   LIKE iceberg.lz.cliente;

-- ============================ ICEBERG: SILVER (upsert) ====================
CREATE TABLE IF NOT EXISTS iceberg.silver.cabecalho (
  nota_id STRING, numero_nota STRING, serie STRING, modelo STRING,
  data_emissao STRING, data_emissao_dia DATE, cliente_id STRING,
  natureza_operacao STRING, valor_total_nota DECIMAL(15,2), status_nota STRING,
  atualizado_em STRING, PRIMARY KEY (nota_id) NOT ENFORCED
) WITH ('format-version'='2','write.upsert.enabled'='true');
CREATE TABLE IF NOT EXISTS iceberg.silver.itens (
  item_id STRING, nota_id STRING, numero_item INT, produto_id STRING,
  descricao_produto STRING, ncm STRING, cfop STRING, quantidade DECIMAL(15,3),
  unidade STRING, valor_unitario DECIMAL(15,2), valor_desconto DECIMAL(15,2),
  valor_bruto DECIMAL(15,2), valor_liquido DECIMAL(15,2), atualizado_em STRING,
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH ('format-version'='2','write.upsert.enabled'='true');
CREATE TABLE IF NOT EXISTS iceberg.silver.impostos (
  imposto_id STRING, nota_id STRING, item_id STRING, tipo_imposto STRING,
  cst STRING, base_calculo DECIMAL(15,2), aliquota DECIMAL(7,4),
  valor_imposto DECIMAL(15,2), atualizado_em STRING,
  PRIMARY KEY (imposto_id) NOT ENFORCED
) WITH ('format-version'='2','write.upsert.enabled'='true');
CREATE TABLE IF NOT EXISTS iceberg.silver.cliente (
  cliente_id STRING, nome_cliente STRING, documento STRING, tipo_pessoa STRING,
  uf STRING, municipio STRING, segmento STRING, atualizado_em STRING,
  PRIMARY KEY (cliente_id) NOT ENFORCED
) WITH ('format-version'='2','write.upsert.enabled'='true');

-- ============================ ICEBERG: GOLD (upsert) ======================
CREATE TABLE IF NOT EXISTS iceberg.gold.nota_fiscal (
  nota_id STRING, numero_nota STRING, serie STRING, data_emissao_dia DATE,
  status_nota STRING, natureza_operacao STRING, cliente_id STRING,
  nome_cliente STRING, uf_cliente STRING, segmento_cliente STRING,
  qtd_itens INT, qtd_produtos DECIMAL(15,3), valor_produtos DECIMAL(15,2),
  valor_descontos DECIMAL(15,2), valor_impostos DECIMAL(15,2),
  valor_total DECIMAL(15,2), atualizado_em STRING,
  PRIMARY KEY (nota_id) NOT ENFORCED
) WITH ('format-version'='2','write.upsert.enabled'='true');

-- ============================ KAFKA SOURCES ===============================
-- LZ (append)
CREATE TEMPORARY TABLE k_lz_cabecalho (op STRING, source_table STRING, lsn BIGINT, ingested_at STRING, nota_id STRING, numero_nota STRING, serie STRING, modelo STRING, data_emissao STRING, cliente_id STRING, natureza_operacao STRING, valor_total DOUBLE, status STRING)
  WITH ('connector'='kafka','topic'='issuance_cabecalho_lz','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-lz-cabecalho','scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');
CREATE TEMPORARY TABLE k_lz_itens (op STRING, source_table STRING, lsn BIGINT, ingested_at STRING, item_id STRING, nota_id STRING, numero_item INT, produto_id STRING, produto_descricao STRING, ncm STRING, cfop STRING, quantidade DOUBLE, unidade STRING, valor_unitario DOUBLE, valor_desconto DOUBLE, valor_total DOUBLE)
  WITH ('connector'='kafka','topic'='issuance_itens_lz','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-lz-itens','scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');
CREATE TEMPORARY TABLE k_lz_impostos (op STRING, source_table STRING, lsn BIGINT, ingested_at STRING, imposto_id STRING, nota_id STRING, item_id STRING, tipo_imposto STRING, cst STRING, base_calculo DOUBLE, aliquota DOUBLE, valor DOUBLE)
  WITH ('connector'='kafka','topic'='issuance_impostos_lz','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-lz-impostos','scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');
CREATE TEMPORARY TABLE k_lz_cliente (op STRING, source_table STRING, lsn BIGINT, ingested_at STRING, cliente_id STRING, nome STRING, documento STRING, tipo_pessoa STRING, email STRING, uf STRING, cidade STRING, segmento STRING)
  WITH ('connector'='kafka','topic'='issuance_cliente_lz','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-lz-cliente','scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');

-- BRONZE (append)
CREATE TEMPORARY TABLE k_br_cabecalho (nota_id STRING, op STRING, lsn BIGINT, ingested_at STRING, numero_nota STRING, serie STRING, modelo STRING, data_emissao STRING, cliente_id STRING, natureza_operacao STRING, valor_total DOUBLE, status STRING)
  WITH ('connector'='kafka','topic'='issuance_cabecalho_bronze','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-br-cabecalho','scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');
CREATE TEMPORARY TABLE k_br_itens (item_id STRING, op STRING, lsn BIGINT, ingested_at STRING, nota_id STRING, numero_item INT, produto_id STRING, produto_descricao STRING, ncm STRING, cfop STRING, quantidade DOUBLE, unidade STRING, valor_unitario DOUBLE, valor_desconto DOUBLE, valor_total DOUBLE)
  WITH ('connector'='kafka','topic'='issuance_itens_bronze','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-br-itens','scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');
CREATE TEMPORARY TABLE k_br_impostos (imposto_id STRING, op STRING, lsn BIGINT, ingested_at STRING, nota_id STRING, item_id STRING, tipo_imposto STRING, cst STRING, base_calculo DOUBLE, aliquota DOUBLE, valor DOUBLE)
  WITH ('connector'='kafka','topic'='issuance_impostos_bronze','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-br-impostos','scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');
CREATE TEMPORARY TABLE k_br_cliente (cliente_id STRING, op STRING, lsn BIGINT, ingested_at STRING, nome STRING, documento STRING, tipo_pessoa STRING, email STRING, uf STRING, cidade STRING, segmento STRING)
  WITH ('connector'='kafka','topic'='issuance_cliente_bronze','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-br-cliente','scan.startup.mode'='earliest-offset','format'='json','json.ignore-parse-errors'='true');

-- SILVER (upsert)
CREATE TEMPORARY TABLE k_sv_cabecalho (nota_id STRING, numero_nota STRING, serie STRING, modelo STRING, data_emissao STRING, data_emissao_dia DATE, cliente_id STRING, natureza_operacao STRING, valor_total_nota DECIMAL(15,2), status_nota STRING, atualizado_em STRING, PRIMARY KEY (nota_id) NOT ENFORCED)
  WITH ('connector'='upsert-kafka','topic'='issuance_cabecalho_silver','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-sv-cabecalho','key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');
CREATE TEMPORARY TABLE k_sv_itens (item_id STRING, nota_id STRING, numero_item INT, produto_id STRING, descricao_produto STRING, ncm STRING, cfop STRING, quantidade DECIMAL(15,3), unidade STRING, valor_unitario DECIMAL(15,2), valor_desconto DECIMAL(15,2), valor_bruto DECIMAL(15,2), valor_liquido DECIMAL(15,2), atualizado_em STRING, PRIMARY KEY (item_id) NOT ENFORCED)
  WITH ('connector'='upsert-kafka','topic'='issuance_itens_silver','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-sv-itens','key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');
CREATE TEMPORARY TABLE k_sv_impostos (imposto_id STRING, nota_id STRING, item_id STRING, tipo_imposto STRING, cst STRING, base_calculo DECIMAL(15,2), aliquota DECIMAL(7,4), valor_imposto DECIMAL(15,2), atualizado_em STRING, PRIMARY KEY (imposto_id) NOT ENFORCED)
  WITH ('connector'='upsert-kafka','topic'='issuance_impostos_silver','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-sv-impostos','key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');
CREATE TEMPORARY TABLE k_sv_cliente (cliente_id STRING, nome_cliente STRING, documento STRING, tipo_pessoa STRING, uf STRING, municipio STRING, segmento STRING, atualizado_em STRING, PRIMARY KEY (cliente_id) NOT ENFORCED)
  WITH ('connector'='upsert-kafka','topic'='issuance_cliente_silver','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-sv-cliente','key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

-- GOLD (upsert)
CREATE TEMPORARY TABLE k_gd_nota (nota_id STRING, numero_nota STRING, serie STRING, data_emissao_dia DATE, status_nota STRING, natureza_operacao STRING, cliente_id STRING, nome_cliente STRING, uf_cliente STRING, segmento_cliente STRING, qtd_itens INT, qtd_produtos DECIMAL(15,3), valor_produtos DECIMAL(15,2), valor_descontos DECIMAL(15,2), valor_impostos DECIMAL(15,2), valor_total DECIMAL(15,2), atualizado_em STRING, PRIMARY KEY (nota_id) NOT ENFORCED)
  WITH ('connector'='upsert-kafka','topic'='issuance_nota_gold','properties.bootstrap.servers'='kafka:9092','properties.group.id'='pl-gd-nota','key.format'='raw','value.format'='json','value.json.ignore-parse-errors'='true');

-- ============================ JOB: persist LZ =============================
BEGIN STATEMENT SET;
INSERT INTO iceberg.lz.cabecalho SELECT op, lsn, ingested_at, nota_id, numero_nota, serie, modelo, data_emissao, cliente_id, natureza_operacao, valor_total, status FROM k_lz_cabecalho;
INSERT INTO iceberg.lz.itens SELECT op, lsn, ingested_at, item_id, nota_id, numero_item, produto_id, produto_descricao, ncm, cfop, quantidade, unidade, valor_unitario, valor_desconto, valor_total FROM k_lz_itens;
INSERT INTO iceberg.lz.impostos SELECT op, lsn, ingested_at, imposto_id, nota_id, item_id, tipo_imposto, cst, base_calculo, aliquota, valor FROM k_lz_impostos;
INSERT INTO iceberg.lz.cliente SELECT op, lsn, ingested_at, cliente_id, nome, documento, tipo_pessoa, email, uf, cidade, segmento FROM k_lz_cliente;
END;

-- ============================ JOB: persist BRONZE =========================
BEGIN STATEMENT SET;
INSERT INTO iceberg.bronze.cabecalho SELECT op, lsn, ingested_at, nota_id, numero_nota, serie, modelo, data_emissao, cliente_id, natureza_operacao, valor_total, status FROM k_br_cabecalho;
INSERT INTO iceberg.bronze.itens SELECT op, lsn, ingested_at, item_id, nota_id, numero_item, produto_id, produto_descricao, ncm, cfop, quantidade, unidade, valor_unitario, valor_desconto, valor_total FROM k_br_itens;
INSERT INTO iceberg.bronze.impostos SELECT op, lsn, ingested_at, imposto_id, nota_id, item_id, tipo_imposto, cst, base_calculo, aliquota, valor FROM k_br_impostos;
INSERT INTO iceberg.bronze.cliente SELECT op, lsn, ingested_at, cliente_id, nome, documento, tipo_pessoa, email, uf, cidade, segmento FROM k_br_cliente;
END;

-- ============================ JOB: persist SILVER =========================
BEGIN STATEMENT SET;
INSERT INTO iceberg.silver.cabecalho SELECT * FROM k_sv_cabecalho;
INSERT INTO iceberg.silver.itens SELECT * FROM k_sv_itens;
INSERT INTO iceberg.silver.impostos SELECT * FROM k_sv_impostos;
INSERT INTO iceberg.silver.cliente SELECT * FROM k_sv_cliente;
END;

-- ============================ JOB: persist GOLD ===========================
INSERT INTO iceberg.gold.nota_fiscal SELECT * FROM k_gd_nota;
