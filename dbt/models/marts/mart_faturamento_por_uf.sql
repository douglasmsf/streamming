-- Faturamento por UF do cliente
select
    coalesce(uf_cliente, 'N/D')   as uf_cliente,
    count(distinct cliente_id)    as qtd_clientes,
    count(*)                      as qtd_notas,
    sum(valor_produtos)           as total_produtos,
    sum(valor_impostos)           as total_impostos,
    sum(valor_total)              as faturamento
from {{ source('gold', 'nota_fiscal') }}
where status_nota <> 'CANCELADA'
group by 1
