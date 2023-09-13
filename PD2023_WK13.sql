with t1 as(
select *, avg(prices) over(partition by sector order by trade_order, sector rows between 2 preceding and current row) as rolling_avg_purchase_price from(
select sector, row_number() over(partition by sector order by sector, "File Date", id) as trade_order, replace(PURCHASE_PRICE, '$', '')::float as prices from til_dataschool.ds28.PD2023_WK08 order by sector, "File Date", id)
)
select row_number() over(partition by sector order by sector, trade_order) as prev_trades, * exclude (num, prices) from(
select row_number() over(partition by sector order by trade_order desc, sector) as num, * from t1
) where num <= 100 order by sector, trade_order;