{{ config(materialized='view') }}

select
'GOOG' as Symbol,
*,
extract(year from fiscalDateEnding) as Year
from {{ ref('goog_cash_flow') }}
where fiscalDateEnding is not null
order by fiscalDateEnding desc
