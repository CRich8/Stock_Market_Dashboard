{{ config(materialized='view') }}
select
'AAPL' as Symbol,
*,
extract(year from fiscalDateEnding) as Year
from {{ ref('aapl_cash_flow') }}
where fiscalDateEnding is not null
order by fiscalDateEnding desc
