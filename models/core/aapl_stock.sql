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
inner join {{ ref('aapl_overview') }} a
on f.symbol = a.symbol
order by date desc
