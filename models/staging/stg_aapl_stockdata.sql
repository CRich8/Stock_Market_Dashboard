{{ config(materialized='view') }}

select
*
from {{ source('staging','aapl_full_12_9') }}