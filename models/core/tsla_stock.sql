{{ config(materialized='table') }}

select 
Date,
Open,
High,
Low,
Close,
Adjusted_Close,
round(close - Lag(close) OVER (ORDER BY date), 2) as Price_Change, 
round(100 * ((close - Lag(close) OVER (ORDER BY date)) / Lag(close) OVER (ORDER BY date)), 2) as Perc_Change,
round(avg(close) over(order by date rows between 2 preceding and current row), 2) as three_day_moving_average,
round(avg(close) over(order by date rows between 29 preceding and current row), 2) as thirty_day_moving_average,
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
_50DayMovingAverage,
grossProfit,
totalRevenue,
costOfRevenue,
netIncome
from {{ ref('fact_stock') }} f
inner join {{ ref('tsla_overview') }} t
on f.symbol = t.symbol
left join {{ ref('stg_tsla_inc_stm') }} i
on f.symbol = i.symbol
and f.year = i.year
order by date desc