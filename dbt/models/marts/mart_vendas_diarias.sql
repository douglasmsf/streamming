-- Evolucao diaria das vendas
select
    data_emissao_dia,
    count(*)                as qtd_notas,
    sum(qtd_itens)          as qtd_itens,
    sum(valor_produtos)     as total_produtos,
    sum(valor_impostos)     as total_impostos,
    sum(valor_total)        as faturamento,
    avg(valor_total)        as ticket_medio
from {{ source('gold', 'nota_fiscal') }}
where status_nota <> 'CANCELADA'
group by 1
order by 1
