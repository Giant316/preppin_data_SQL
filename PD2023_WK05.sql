-- use decode function to translate the month number to full month name (monthname function only returns abbreviated month name)
-- rank each bank against the other banks for their value of transaction value using partition in rank function
-- average rank of each bank across all of the months, average transaction value of each rank using partition in average function
with WK05 as(
select split_part(TRANSACTION_CODE, '-', 1) as "Bank", decode(split_part(TRANSACTION_DATE, '/', 2), 
'01', 'January',
'02', 'February',
'03', 'March',
'04', 'April',
'05', 'May',
'06', 'June',
'07', 'July',
'08', 'August',
'09', 'September',
'10', 'October',
'11', 'November',
'12', 'December'
)  as "Transaction Date", sum(VALUE) as "Value", rank() over(partition by "Transaction Date" order by "Value" desc)::STRING as "Bank Rank per Month" from PD2023_WK01 group by "Bank", "Transaction Date" order by "Transaction Date", "Bank"
)
select "Transaction Date", "Bank", "Value", "Bank Rank per Month", avg("Value") over(partition by "Bank Rank per Month") as "Avg Transaction Value per Rank", avg("Bank Rank per Month") over(partition by "Bank") as "Avg Rank per Bank" from WK05
