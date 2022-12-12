{{ config(materialized='view') }}

select
Date,
Open,
High,
Low,
Close,
Adjusted_Close,
Volume,
Dividend_Amount,
Split_coefficient,
'TSLA' as Symbol
from {{ source('staging','tsla_full_12_9') }}
where date is not null
order by date desc
