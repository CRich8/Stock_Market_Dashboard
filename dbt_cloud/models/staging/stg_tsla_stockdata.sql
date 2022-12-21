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
'TSLA' as Symbol,
extract(year from date) as Year
from {{ source('staging','tsla_daily_stock_external_table') }}
where date is not null
order by date desc
