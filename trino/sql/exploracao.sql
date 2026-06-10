-- Consultas de exemplo para rodar no Trino
-- (CLI:  docker compose exec trino trino)

SHOW SCHEMAS FROM iceberg;   -- lz, bronze, silver, gold, semantic

-- ---- LZ (landing zone, bruto - "Landing bucket") ----
SELECT op, count(*) FROM iceberg.lz.cabecalho GROUP BY op;

-- ---- BRONZE (padronizado, append) ----
SELECT count(*) FROM iceberg.bronze.itens;

-- ---- SILVER (deduplicado por PK / ultima versao) ----
SELECT * FROM iceberg.silver.cabecalho ORDER BY atualizado_em DESC LIMIT 20;
SELECT * FROM iceberg.silver.cliente LIMIT 20;

-- ---- GOLD (nota fiscal consolidada em tempo real) ----
SELECT nota_id, nome_cliente, uf_cliente, status_nota,
       qtd_itens, valor_produtos, valor_impostos, valor_total
FROM iceberg.gold.nota_fiscal
ORDER BY atualizado_em DESC LIMIT 25;

SELECT count(*) AS qtd_notas, sum(valor_total) AS faturamento
FROM iceberg.gold.nota_fiscal WHERE status_nota <> 'CANCELADA';

-- ---- SEMANTIC (marts gerados pelo dbt) ----
SHOW TABLES FROM iceberg.semantic;
SELECT * FROM iceberg.semantic.mart_faturamento_por_uf ORDER BY faturamento DESC;
