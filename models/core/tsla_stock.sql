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
i.netIncome,
c.operatingCashflow,
c.paymentsForOperatingActivities,
c.profitLoss,
c.changeInOperatingLiabilities,
c.changeInOperatingAssets,
round(f.close / o.DilutedEPSTTM,2) as PE
from {{ ref('fact_stock') }} f
inner join {{ ref('tsla_overview') }} o
on f.symbol = o.symbol
left join {{ ref('stg_tsla_inc_stm') }} i
on f.symbol = i.symbol
and f.year = i.year
left join {{ ref('stg_tsla_cash_flow') }} c
on f.symbol = c.symbol
and f.year = c.year
order by date desc