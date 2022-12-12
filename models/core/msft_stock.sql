{{ config(materialized='table') }}

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
f.Symbol,
MarketCapitalization,
Description,
Sector,
GrossProfitTTM,
QuarterlyEarningsGrowthYOY,
_52WeekHigh,
_52WeekLow,
_50DayMovingAverage
from {{ ref('fact_stock') }} f
inner join {{ ref('msft_overview') }} m
on f.symbol = m.symbol
order by date desc