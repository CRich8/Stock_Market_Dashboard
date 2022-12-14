{{ config(materialized='view') }}

select
'GOOG' as Symbol,
fiscalDateEnding,
grossProfit,
totalRevenue,
costOfRevenue,
costofGoodsAndServicesSold,
netIncome,
extract(year from fiscalDateEnding) as Year
from {{ source('core','goog_inc_stm') }}
where fiscalDateEnding is not null
order by fiscalDateEnding desc
