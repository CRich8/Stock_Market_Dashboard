{{ config(materialized='view') }}

select
'TSLA' as Symbol,
fiscalDateEnding,
grossProfit,
totalRevenue,
costOfRevenue,
costofGoodsAndServicesSold,
netIncome,
operatingIncome,
ebitda,
extract(year from fiscalDateEnding) as Year
from {{ source('core','tsla_inc_stm') }}
where fiscalDateEnding is not null
order by fiscalDateEnding desc
