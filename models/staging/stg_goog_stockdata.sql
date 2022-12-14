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
'GOOG' as Symbol,
extract(year from date) as Year
from {{ source('staging','goog_full_12_9') }}
where date is not null
order by date desc
