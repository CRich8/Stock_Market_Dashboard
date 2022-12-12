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
'MSFT' as Symbol
from {{ source('staging','msft_full_12_9') }}
where date is not null
order by date desc
