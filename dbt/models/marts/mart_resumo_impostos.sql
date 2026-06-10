-- Resumo de impostos por tipo
select
    tipo_imposto,
    count(*)                  as qtd_lancamentos,
    count(distinct nota_id)   as qtd_notas,
    sum(base_calculo)         as total_base_calculo,
    avg(aliquota)             as aliquota_media,
    sum(valor_imposto)        as total_imposto
from {{ source('silver', 'impostos') }}
group by 1
order by total_imposto desc
