{{ config(materialized='table') }}

select 
*
from {{ ref('stg_aapl_stockdata') }}
union all 
select * from {{ ref('stg_goog_stockdata') }}
union all 
select * from {{ ref('stg_msft_stockdata') }}
union all 
select * from {{ ref('stg_tsla_stockdata') }}
