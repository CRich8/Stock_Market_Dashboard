{{ config(materialized='table') }}
select 
*
from {{ ref('aapl_stock') }}
union all 
select * from {{ ref('goog_stock') }}
union all 
select * from {{ ref('msft_stock') }}
union all 
select * from {{ ref('tsla_stock') }}
