-- Ranking de produtos por quantidade e valor vendido (camada silver de itens)
select
    produto_id,
    descricao_produto,
    count(distinct nota_id)   as qtd_notas,
    sum(quantidade)           as qtd_vendida,
    sum(valor_liquido)        as valor_vendido,
    sum(valor_desconto)       as total_descontos
from {{ source('silver', 'itens') }}
group by 1, 2
order by valor_vendido desc
