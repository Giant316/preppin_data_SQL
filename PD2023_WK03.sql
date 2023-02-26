-- Assign the Online or In-Person string based on its value
-- Convert the transaction date format to timestamp and then to be converted to Quarter
-- Filter the query to only include DSB transaction code
with WK01_FT as (
select iff(online_or_in_person = 1, 'Online', 'In-Person') as "Online or In-Person", quarter(concat_ws('-', right(left(TRANSACTION_DATE, 10),4), right(left(TRANSACTION_DATE, 5),2), left(left(TRANSACTION_DATE, 10),2))::TIMESTAMP_NTZ) as "Quarter", sum(value) as "Value" from PD2023_WK01 where contains(TRANSACTION_CODE, 'DSB') group by "Online or In-Person", "Quarter"
),
-- Pivot the Quarterly Targets table to match the data structure of the financial transaction in Week 1
-- Extract the Quarter Number to be used as join condition
quarter_targets as(
select *, right(quarter, 1)::NUMBER as QUARTER_NUM from PD2023_WK03_TARGETS unpivot(quarter_target for quarter in (Q1, Q2, Q3, Q4))
)
-- join the two tables on Quarter and Online/In-Person type 
-- calculate the variance to target field
select WK01_FT.*, QUARTER_TARGET as "Quarterly Targets", "Value" - "Quarterly Targets" as "Variance to Target" from quarter_targets as qt inner join WK01_FT on qt.QUARTER_NUM = WK01_FT."Quarter" and qt.ONLINE_OR_IN_PERSON = WK01_FT."Online or In-Person" 