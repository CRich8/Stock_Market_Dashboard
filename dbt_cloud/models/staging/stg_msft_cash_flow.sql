{{ config(materialized='view') }}

select
'MSFT' as Symbol,
*,
extract(year from fiscalDateEnding) as Year
from {{ ref('msft_cash_flow') }}
where fiscalDateEnding is not null
order by fiscalDateEnding desc
