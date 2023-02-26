-- create a view in the schema that stores all the union-ed tables 
create view PD2023_WK08 as
with WK08 as(
select *, to_date('2023-01-01') as "File Date" from PD2023_WK08_JANUARY union all
select *, to_date('2023-02-01') as "File Date" from PD2023_WK08_FEBRUARY union all
select *, to_date('2023-03-01') as "File Date" from PD2023_WK08_MARCH union all
select *, to_date('2023-04-01') as "File Date" from PD2023_WK08_APRIL union all
select *, to_date('2023-05-01') as "File Date" from PD2023_WK08_MAY union all
select *, to_date('2023-06-01') as "File Date" from PD2023_WK08_JUNE union all
select *, to_date('2023-07-01') as "File Date" from PD2023_WK08_JULY union all
select *, to_date('2023-08-01') as "File Date" from PD2023_WK08_AUGUST union all
select *, to_date('2023-09-01') as "File Date" from PD2023_WK08_SEPTEMBER union all
select *, to_date('2023-10-01') as "File Date" from PD2023_WK08_OCTOBER union all
select *, to_date('2023-11-01') as "File Date" from PD2023_WK08_NOVEMBER union all
select *, to_date('2023-12-01') as "File Date" from PD2023_WK08_DECEMBER
)
select * from WK08
---------------------------------------------------------------------------------------------------------------------
-- use window function to rank the highest 5 purchases per combination of: file date, Purchase Price Categorisation and Market Capitalisation 
-- to include only the rank (1-5) per combination, use CTE to perform rank filter (window function cannot be filtered directly)
-- use CASE clause to categorise market cap/purchase price groups
with DPWK08 as(
select *, 
case 
	when endswith(MARKET_CAP, 'B') and contains(MARKET_CAP, '.') then regexp_substr(MARKET_CAP, '\\d+\\.\\d+')::number*1000000000
	when endswith(MARKET_CAP, 'M') and contains(MARKET_CAP, '.') then regexp_substr(MARKET_CAP, '\\d+\\.\\d+')::number*1000000
    when endswith(MARKET_CAP, 'M') or endswith(MARKET_CAP, 'B') then replace(left(MARKET_CAP, length(MARKET_CAP)-1), '$', '')
    when not endswith(MARKET_CAP, 'M') and not endswith(MARKET_CAP, 'B') then replace(MARKET_CAP, '$', '')
end as "Market Capitalisation", 
replace(PURCHASE_PRICE, '$', '')::number as PRICE, 
case
	when PRICE <= 24999.99  then 'Small'
    when PRICE >= 25000 and PRICE <= 49999.99 then 'Medium'
    when PRICE >= 50000 and PRICE <= 74999.99 then 'Large'
    when PRICE >= 75000 then 'Very Large'
end as "Purchase Price Categorisation",
case
	when "Market Capitalisation" < 100000000 then 'Small'
    when "Market Capitalisation" >= 100000000 and "Market Capitalisation" < 1000000000 then'Medium'
    when "Market Capitalisation" >= 1000000000 and "Market Capitalisation" <100000000000 then 'Large'
    when "Market Capitalisation" >= 100000000000 then 'Huge'
end as "Market Capitalisation Categorisation",
rank() over(partition by "File Date", "Purchase Price Categorisation", "Market Capitalisation Categorisation" order by PURCHASE_PRICE desc) as "Rank" from PD2023_WK08 where MARKET_CAP <> 'n/a' order by "File Date", "Purchase Price Categorisation", "Market Capitalisation Categorisation", "Rank"
)
select "Market Capitalisation Categorisation", "Purchase Price Categorisation", "File Date", TICKER as "Ticker", SECTOR as "Sector", MARKET as "Market", STOCK_NAME as "Stock Name", "Market Capitalisation", PRICE as "Purchase Price", "Rank" from DPWK08 where "Rank" < 6
