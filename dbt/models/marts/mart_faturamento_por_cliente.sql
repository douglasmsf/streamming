-- Faturamento consolidado por cliente (exclui notas canceladas)
select
    cliente_id,
    nome_cliente,
    uf_cliente,
    segmento_cliente,
    count(*)                as qtd_notas,
    sum(qtd_itens)          as qtd_itens,
    sum(valor_produtos)     as total_produtos,
    sum(valor_impostos)     as total_impostos,
    sum(valor_total)        as faturamento
from {{ source('gold', 'nota_fiscal') }}
where status_nota <> 'CANCELADA'
group by 1, 2, 3, 4
