{{ config(materialized='view') }}

select
'TSLA' as Symbol,
*,
extract(year from fiscalDateEnding) as Year
from {{ ref('tsla_cash_flow') }}
where fiscalDateEnding is not null
order by fiscalDateEnding desc
