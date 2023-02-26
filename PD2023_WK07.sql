-- split joint account ids concatenated with delimiter into rows using split_to_table function
-- combine multiple tables into one
with PATH_DETAIL as(
select tp.*, td.* exclude (TRANSACTION_ID, CANCELLED_) from PD2023_WK07_TRANSACTION_PATH as tp inner join PD2023_WK07_TRANSACTION_DETAIL as td on tp.TRANSACTION_ID = td.TRANSACTION_ID where CANCELLED_ <> 'Y' and VALUE > 1000
),
PATH_DETAIL_INFO as(
select PATH_DETAIL.* exclude ACCOUNT_FROM, ai.* exclude ACCOUNT_HOLDER_ID from PATH_DETAIL inner join PD2023_WK07_ACCOUNT_INFORMATION as ai on PATH_DETAIL.ACCOUNT_FROM = ai.ACCOUNT_NUMBER
),
INFO_HOLDERS as(
select * from PD2023_WK07_ACCOUNT_INFORMATION, lateral split_to_table(ACCOUNT_HOLDER_ID, ', ') as ai inner join PD2023_WK07_ACCOUNT_HOLDERS as ah on ai.VALUE = ah.ACCOUNT_HOLDER_ID
),
COMBINE_ALL as(
select ACCOUNT_NUMBER as ACCOUNT_NUMBER2, NAME, DATE_OF_BIRTH, CONTACT_NUMBER, FIRST_LINE_OF_ADDRESS from INFO_HOLDERS
)
select PATH_DETAIL_INFO.*, COMBINE_ALL.* exclude ACCOUNT_NUMBER2 from PATH_DETAIL_INFO inner join COMBINE_ALL on PATH_DETAIL_INFO.ACCOUNT_NUMBER = COMBINE_ALL.ACCOUNT_NUMBER2 where ACCOUNT_TYPE <> 'Platinum'